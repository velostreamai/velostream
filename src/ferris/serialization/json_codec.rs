//! JSON codec for HashMap<String, FieldValue> serialization/deserialization

use crate::ferris::kafka::serialization::Serializer;
use crate::ferris::serialization::helpers::json_to_field_value;
use crate::ferris::serialization::SerializationError;
use crate::ferris::sql::execution::types::FieldValue;
use serde_json::Value;
use std::collections::HashMap;

/// JSON codec that deserializes to HashMap<String, FieldValue>
/// This enables unified consumer types across all serialization formats
pub struct JsonCodec;

impl JsonCodec {
    /// Create a new JsonCodec
    pub fn new() -> Self {
        JsonCodec
    }
}

impl Serializer<HashMap<String, FieldValue>> for JsonCodec {
    /// Serialize HashMap<String, FieldValue> to JSON bytes
    fn serialize(
        &self,
        value: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        // Convert HashMap<String, FieldValue> to serde_json::Value
        let mut json_obj = serde_json::Map::new();

        for (key, field_value) in value {
            let json_value = field_value_to_json_value(field_value);
            json_obj.insert(key.clone(), json_value);
        }

        let json_value = Value::Object(json_obj);
        serde_json::to_vec(&json_value)
            .map_err(|e| SerializationError::json_error("Failed to serialize JSON", e))
    }

    /// Deserialize JSON bytes to HashMap<String, FieldValue>
    fn deserialize(&self, bytes: &[u8]) -> Result<HashMap<String, FieldValue>, SerializationError> {
        // Store the original JSON string as JSON_PAYLOAD field
        let json_payload = String::from_utf8(bytes.to_vec())
            .map_err(|e| SerializationError::encoding_error("Invalid UTF-8 in JSON data", e))?;

        // Parse JSON bytes to serde_json::Value
        let json_value: Value = serde_json::from_slice(bytes)
            .map_err(|e| SerializationError::json_error("Failed to parse JSON from bytes", e))?;

        let mut fields = HashMap::new();

        // Always add the original JSON string as JSON_PAYLOAD
        fields.insert("JSON_PAYLOAD".to_string(), FieldValue::String(json_payload));

        match json_value {
            Value::Object(obj) => {
                // If it's a JSON object, add each field
                for (key, value) in obj {
                    let field_value = json_to_field_value(&value).map_err(|e| {
                        SerializationError::type_conversion_error(
                            format!("Field conversion failed: {}", e),
                            "JSON",
                            "FieldValue",
                            Some(e),
                        )
                    })?;
                    fields.insert(key, field_value);
                }
            }
            _ => {
                // If it's not an object, store as single "value" field
                let field_value = json_to_field_value(&json_value).map_err(|e| {
                    SerializationError::type_conversion_error(
                        format!("Value conversion failed: {}", e),
                        "JSON",
                        "FieldValue",
                        Some(e),
                    )
                })?;
                fields.insert("value".to_string(), field_value);
            }
        }

        Ok(fields)
    }
}

/// Convert FieldValue to serde_json::Value for serialization
fn field_value_to_json_value(field_value: &FieldValue) -> Value {
    match field_value {
        FieldValue::String(s) => Value::String(s.clone()),
        FieldValue::Integer(i) => Value::Number(serde_json::Number::from(*i)),
        FieldValue::Float(f) => {
            if let Some(num) = serde_json::Number::from_f64(*f) {
                Value::Number(num)
            } else {
                Value::Null
            }
        }
        FieldValue::Boolean(b) => Value::Bool(*b),
        FieldValue::Null => Value::Null,
        FieldValue::ScaledInteger(val, scale) => {
            // Serialize ScaledInteger as decimal string for JSON compatibility
            let divisor = 10_i64.pow(*scale as u32);
            let integer_part = val / divisor;
            let fractional_part = (val % divisor).abs();
            if fractional_part == 0 {
                Value::String(integer_part.to_string())
            } else {
                let frac_str = format!("{:0width$}", fractional_part, width = *scale as usize);
                let frac_trimmed = frac_str.trim_end_matches('0');
                if frac_trimmed.is_empty() {
                    Value::String(integer_part.to_string())
                } else {
                    Value::String(format!("{}.{}", integer_part, frac_trimmed))
                }
            }
        }
        FieldValue::Date(date) => Value::String(date.format("%Y-%m-%d").to_string()),
        FieldValue::Timestamp(datetime) => {
            Value::String(datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        }
        FieldValue::Decimal(decimal) => Value::String(decimal.to_string()),
        FieldValue::Array(array) => {
            let json_array: Vec<Value> = array.iter().map(field_value_to_json_value).collect();
            Value::Array(json_array)
        }
        FieldValue::Map(map) => {
            let mut json_obj = serde_json::Map::new();
            for (key, value) in map {
                json_obj.insert(key.clone(), field_value_to_json_value(value));
            }
            Value::Object(json_obj)
        }
        FieldValue::Struct(map) => {
            let mut json_obj = serde_json::Map::new();
            for (key, value) in map {
                json_obj.insert(key.clone(), field_value_to_json_value(value));
            }
            Value::Object(json_obj)
        }
        FieldValue::Interval { value, unit } => {
            // Serialize interval as object with value and unit
            let mut json_obj = serde_json::Map::new();
            json_obj.insert(
                "value".to_string(),
                Value::Number(serde_json::Number::from(*value)),
            );
            json_obj.insert("unit".to_string(), Value::String(format!("{:?}", unit)));
            Value::Object(json_obj)
        }
    }
}

impl Default for JsonCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// Implementation of UnifiedCodec for runtime abstraction
impl crate::ferris::serialization::traits::UnifiedCodec for JsonCodec {
    fn serialize_record(
        &self,
        value: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        self.serialize(value)
    }

    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        self.deserialize(bytes)
    }

    fn format_name(&self) -> &'static str {
        "JSON"
    }
}
