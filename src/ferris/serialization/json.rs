//! JSON serialization format implementation

use super::helpers::{field_value_to_json, json_to_field_value};
use super::{FieldValue, SerializationError, SerializationFormat};
use std::collections::HashMap;

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

    fn format_name(&self) -> &'static str {
        "JSON"
    }
}
