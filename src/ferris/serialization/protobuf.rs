//! Protobuf serialization format implementation

use super::helpers::{
    field_value_to_json, json_to_field_value
};
use super::{FieldValue, SerializationError, SerializationFormat};
use std::collections::HashMap;

/// Protocol Buffers serialization implementation (feature-gated)
pub struct ProtobufFormat<T>
where
    T: prost::Message + Default,
{
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Default for ProtobufFormat<T>
where
    T: prost::Message + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ProtobufFormat<T>
where
    T: prost::Message + Default,
{
    /// Create new Protobuf format
    pub fn new() -> Self {
        ProtobufFormat {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> SerializationFormat for ProtobufFormat<T>
where
    T: prost::Message + Default + Clone + Send + Sync + 'static,
{
    fn serialize_record(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        // For generic protobuf support, we'll encode as a map-like structure
        // In practice, you'd want to use specific message types
        let mut buf = Vec::new();

        // Convert record to JSON first, then to bytes (simplified approach)
        let json_value = record_to_json_map(record)?;
        let json_bytes = serde_json::to_vec(&json_value)
            .map_err(|e| SerializationError::SerializationFailed(e.to_string()))?;

        // For a generic implementation, we'll store JSON as bytes
        // Real implementation would use proper protobuf message definitions
        buf.extend_from_slice(&json_bytes);
        Ok(buf)
    }

    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        // For generic protobuf support, decode from JSON bytes
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
                "Expected JSON object from protobuf data".to_string(),
            )),
        }
    }

    fn format_name(&self) -> &'static str {
        "Protobuf"
    }
}

// Helper function
fn record_to_json_map(
    record: &HashMap<String, FieldValue>,
) -> Result<serde_json::Map<String, serde_json::Value>, SerializationError> {
    let mut json_map = serde_json::Map::new();

    for (key, field_value) in record {
        let json_value = field_value_to_json(field_value)?;
        json_map.insert(key.clone(), json_value);
    }

    Ok(json_map)
}
