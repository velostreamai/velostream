//! Pluggable serialization interface for FerrisStreams
//!
//! This module provides a trait-based approach to serialization that allows
//! the system to work with multiple data formats (JSON, Avro, Protobuf, etc.)
//! instead of being hardcoded to JSON.

use crate::ferris::sql::{FieldValue, SqlError};
use std::collections::HashMap;

/// Trait for pluggable serialization formats
pub trait SerializationFormat: Send + Sync {
    /// Serialize a record to bytes for Kafka production
    fn serialize_record(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError>;

    /// Deserialize bytes from Kafka into a record
    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError>;

    /// Convert record to internal execution format (HashMap<String, InternalValue>)
    fn to_execution_format(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<HashMap<String, InternalValue>, SerializationError>;

    /// Convert from internal execution format back to record
    fn from_execution_format(
        &self,
        data: &HashMap<String, InternalValue>,
    ) -> Result<HashMap<String, FieldValue>, SerializationError>;

    /// Get the format name (for logging/debugging)
    fn format_name(&self) -> &'static str;
}

/// Internal value type for SQL execution (replaces hardcoded serde_json::Value)
#[derive(Debug, Clone, PartialEq)]
pub enum InternalValue {
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
    Null,
    Array(Vec<InternalValue>),
    Object(HashMap<String, InternalValue>),
}

/// Serialization error type
#[derive(Debug)]
pub enum SerializationError {
    SerializationFailed(String),
    DeserializationFailed(String),
    FormatConversionFailed(String),
    UnsupportedType(String),
}

impl std::fmt::Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerializationError::SerializationFailed(msg) => {
                write!(f, "Serialization failed: {}", msg)
            }
            SerializationError::DeserializationFailed(msg) => {
                write!(f, "Deserialization failed: {}", msg)
            }
            SerializationError::FormatConversionFailed(msg) => {
                write!(f, "Format conversion failed: {}", msg)
            }
            SerializationError::UnsupportedType(msg) => write!(f, "Unsupported type: {}", msg),
        }
    }
}

impl std::error::Error for SerializationError {}

/// Convert SerializationError to SqlError
impl From<SerializationError> for SqlError {
    fn from(err: SerializationError) -> Self {
        SqlError::ExecutionError {
            message: format!("Serialization error: {}", err),
            query: None,
        }
    }
}

/// JSON implementation of SerializationFormat
pub struct JsonFormat;

impl SerializationFormat for JsonFormat {
    fn serialize_record(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        // Convert FieldValue to serde_json::Value
        let mut json_map = serde_json::Map::new();

        for (key, field_value) in record {
            let json_value = field_value_to_json(field_value)?;
            json_map.insert(key.clone(), json_value);
        }

        let json_object = serde_json::Value::Object(json_map);
        serde_json::to_vec(&json_object)
            .map_err(|e| SerializationError::SerializationFailed(e.to_string()))
    }

    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        let json_value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|e| SerializationError::DeserializationFailed(e.to_string()))?;

        match json_value {
            serde_json::Value::Object(map) => {
                let mut record = HashMap::new();
                for (key, value) in map {
                    let field_value = json_to_field_value(&value)?;
                    record.insert(key, field_value);
                }
                Ok(record)
            }
            _ => Err(SerializationError::DeserializationFailed(
                "Expected JSON object".to_string(),
            )),
        }
    }

    fn to_execution_format(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<HashMap<String, InternalValue>, SerializationError> {
        let mut execution_map = HashMap::new();

        for (key, field_value) in record {
            let internal_value = field_value_to_internal(field_value)?;
            execution_map.insert(key.clone(), internal_value);
        }

        Ok(execution_map)
    }

    fn from_execution_format(
        &self,
        data: &HashMap<String, InternalValue>,
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        let mut record = HashMap::new();

        for (key, internal_value) in data {
            let field_value = internal_to_field_value(internal_value)?;
            record.insert(key.clone(), field_value);
        }

        Ok(record)
    }

    fn format_name(&self) -> &'static str {
        "JSON"
    }
}

// Helper functions for JSON conversion

fn field_value_to_json(field_value: &FieldValue) -> Result<serde_json::Value, SerializationError> {
    match field_value {
        FieldValue::String(s) => Ok(serde_json::Value::String(s.clone())),
        FieldValue::Integer(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
        FieldValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                SerializationError::FormatConversionFailed(format!("Invalid float: {}", f))
            }),
        FieldValue::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
        FieldValue::Null => Ok(serde_json::Value::Null),
        FieldValue::Array(arr) => {
            let json_arr: Result<Vec<_>, _> = arr.iter().map(field_value_to_json).collect();
            Ok(serde_json::Value::Array(json_arr?))
        }
        FieldValue::Map(map) => {
            let mut json_map = serde_json::Map::new();
            for (k, v) in map {
                json_map.insert(k.clone(), field_value_to_json(v)?);
            }
            Ok(serde_json::Value::Object(json_map))
        }
        FieldValue::Struct(fields) => {
            let mut json_map = serde_json::Map::new();
            for (k, v) in fields {
                json_map.insert(k.clone(), field_value_to_json(v)?);
            }
            Ok(serde_json::Value::Object(json_map))
        }
    }
}

fn json_to_field_value(json_value: &serde_json::Value) -> Result<FieldValue, SerializationError> {
    match json_value {
        serde_json::Value::String(s) => Ok(FieldValue::String(s.clone())),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(FieldValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(FieldValue::Float(f))
            } else {
                Ok(FieldValue::String(n.to_string()))
            }
        }
        serde_json::Value::Bool(b) => Ok(FieldValue::Boolean(*b)),
        serde_json::Value::Null => Ok(FieldValue::Null),
        serde_json::Value::Array(arr) => {
            let field_arr: Result<Vec<_>, _> = arr.iter().map(json_to_field_value).collect();
            Ok(FieldValue::Array(field_arr?))
        }
        serde_json::Value::Object(obj) => {
            let mut field_map = HashMap::new();
            for (k, v) in obj {
                field_map.insert(k.clone(), json_to_field_value(v)?);
            }
            Ok(FieldValue::Map(field_map))
        }
    }
}

fn field_value_to_internal(field_value: &FieldValue) -> Result<InternalValue, SerializationError> {
    match field_value {
        FieldValue::String(s) => Ok(InternalValue::String(s.clone())),
        FieldValue::Integer(i) => Ok(InternalValue::Integer(*i)),
        FieldValue::Float(f) => Ok(InternalValue::Number(*f)),
        FieldValue::Boolean(b) => Ok(InternalValue::Boolean(*b)),
        FieldValue::Null => Ok(InternalValue::Null),
        FieldValue::Array(arr) => {
            let internal_arr: Result<Vec<_>, _> = arr.iter().map(field_value_to_internal).collect();
            Ok(InternalValue::Array(internal_arr?))
        }
        FieldValue::Map(map) => {
            let mut internal_map = HashMap::new();
            for (k, v) in map {
                internal_map.insert(k.clone(), field_value_to_internal(v)?);
            }
            Ok(InternalValue::Object(internal_map))
        }
        FieldValue::Struct(fields) => {
            let mut internal_map = HashMap::new();
            for (k, v) in fields {
                internal_map.insert(k.clone(), field_value_to_internal(v)?);
            }
            Ok(InternalValue::Object(internal_map))
        }
    }
}

fn internal_to_field_value(
    internal_value: &InternalValue,
) -> Result<FieldValue, SerializationError> {
    match internal_value {
        InternalValue::String(s) => Ok(FieldValue::String(s.clone())),
        InternalValue::Integer(i) => Ok(FieldValue::Integer(*i)),
        InternalValue::Number(f) => Ok(FieldValue::Float(*f)),
        InternalValue::Boolean(b) => Ok(FieldValue::Boolean(*b)),
        InternalValue::Null => Ok(FieldValue::Null),
        InternalValue::Array(arr) => {
            let field_arr: Result<Vec<_>, _> = arr.iter().map(internal_to_field_value).collect();
            Ok(FieldValue::Array(field_arr?))
        }
        InternalValue::Object(obj) => {
            let mut field_map = HashMap::new();
            for (k, v) in obj {
                field_map.insert(k.clone(), internal_to_field_value(v)?);
            }
            Ok(FieldValue::Map(field_map))
        }
    }
}

/// Factory for creating serialization formats
pub struct SerializationFormatFactory;

impl SerializationFormatFactory {
    /// Create a serialization format by name
    pub fn create_format(
        format_name: &str,
    ) -> Result<Box<dyn SerializationFormat>, SerializationError> {
        match format_name.to_lowercase().as_str() {
            "json" => Ok(Box::new(JsonFormat)),
            _ => Err(SerializationError::UnsupportedType(format!(
                "Unknown format: {}",
                format_name
            ))),
        }
    }

    /// Get list of supported formats
    pub fn supported_formats() -> Vec<&'static str> {
        vec!["json"]
    }

    /// Get default format (JSON)
    pub fn default_format() -> Box<dyn SerializationFormat> {
        Box::new(JsonFormat)
    }
}
