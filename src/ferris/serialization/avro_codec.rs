//! Avro codec for HashMap<String, FieldValue> serialization/deserialization

use crate::ferris::kafka::serialization::Serializer;
use crate::ferris::serialization;
use crate::ferris::serialization::SerializationError;
use crate::ferris::sql::execution::types::FieldValue;
use apache_avro::{types::Value as AvroValue, Reader, Schema as AvroSchema, Writer};
use std::collections::HashMap;

/// Avro codec for serializing/deserializing HashMap<String, FieldValue> using a schema
pub struct AvroCodec {
    schema: AvroSchema,
}

impl AvroCodec {
    /// Create a new AvroCodec with the given schema JSON
    pub fn new(schema_json: &str) -> Result<Self, SerializationError> {
        let schema = AvroSchema::parse_str(schema_json)
            .map_err(|e| SerializationError::avro_error("Failed to parse Avro schema", e))?;

        Ok(AvroCodec { schema })
    }

    /// Create a new AvroCodec with a pre-parsed schema
    pub fn with_schema(schema: AvroSchema) -> Self {
        AvroCodec { schema }
    }

    /// Serialize a HashMap<String, FieldValue> to Avro bytes
    pub fn serialize(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        // Convert HashMap to Avro Value
        let avro_value = self.record_to_avro_value(record)?;

        // Create writer and serialize
        let mut writer = Writer::new(&self.schema, Vec::new());
        writer.append(avro_value).map_err(|e| {
            SerializationError::avro_error("Failed to append Avro data to writer", e)
        })?;

        writer.into_inner().map_err(|e| {
            SerializationError::avro_error("Failed to extract serialized Avro bytes", e)
        })
    }

    /// Deserialize Avro bytes to HashMap<String, FieldValue>
    pub fn deserialize(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        let mut reader = Reader::with_schema(&self.schema, bytes)
            .map_err(|e| SerializationError::avro_error("Failed to create Avro reader", e))?;

        // Read the first record
        if let Some(record_result) = reader.next() {
            let avro_value = record_result
                .map_err(|e| SerializationError::avro_error("Failed to read Avro record", e))?;

            self.avro_value_to_record(&avro_value)
        } else {
            Err(SerializationError::type_conversion_error(
                "No records found in Avro data".to_string(),
                "AvroData",
                "Record",
                None::<std::io::Error>,
            ))
        }
    }

    /// Get the schema used by this codec
    pub fn schema(&self) -> &AvroSchema {
        &self.schema
    }

    /// Convert HashMap<String, FieldValue> to Avro Value
    fn record_to_avro_value(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<AvroValue, SerializationError> {
        let mut avro_fields = Vec::new();

        for (key, field_value) in record {
            let avro_value = self.field_value_to_avro(field_value)?;
            avro_fields.push((key.clone(), avro_value));
        }

        Ok(AvroValue::Record(avro_fields))
    }

    /// Convert FieldValue to Avro Value
    fn field_value_to_avro(
        &self,
        field_value: &FieldValue,
    ) -> Result<AvroValue, SerializationError> {
        match field_value {
            FieldValue::Null => Ok(AvroValue::Null),
            FieldValue::Boolean(b) => Ok(AvroValue::Boolean(*b)),
            FieldValue::Integer(i) => Ok(AvroValue::Long(*i)),
            FieldValue::Float(f) => Ok(AvroValue::Double(*f)),
            FieldValue::String(s) => Ok(AvroValue::String(s.clone())),
            FieldValue::ScaledInteger(value, scale) => {
                // Convert ScaledInteger to decimal string representation by default
                let scale_factor = 10_i64.pow(*scale as u32);
                let _decimal_value = *value as f64 / scale_factor as f64;

                // For now, convert to string representation (schema-compatible)
                // TODO: Make this schema-aware to return Float/Double when appropriate
                let integer_part = value / scale_factor;
                let fractional_part = (value % scale_factor).abs();

                let decimal_string = if fractional_part == 0 {
                    integer_part.to_string()
                } else {
                    let frac_str = format!("{:0width$}", fractional_part, width = *scale as usize);
                    let frac_trimmed = frac_str.trim_end_matches('0');
                    if frac_trimmed.is_empty() {
                        integer_part.to_string()
                    } else {
                        format!("{}.{}", integer_part, frac_trimmed)
                    }
                };

                Ok(AvroValue::String(decimal_string))
            }
            FieldValue::Timestamp(ts) => {
                // Convert to ISO 8601 string format for schema compatibility
                Ok(AvroValue::String(
                    ts.format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
                ))
            }
            FieldValue::Array(arr) => {
                let mut avro_array = Vec::new();
                for item in arr {
                    avro_array.push(self.field_value_to_avro(item)?);
                }
                Ok(AvroValue::Array(avro_array))
            }
            FieldValue::Map(map) => {
                let mut avro_map = HashMap::new();
                for (k, v) in map {
                    let avro_value = self.field_value_to_avro(v)?;
                    avro_map.insert(k.clone(), avro_value);
                }
                Ok(AvroValue::Map(avro_map))
            }
            FieldValue::Date(date) => {
                // Convert to YYYY-MM-DD string format for schema compatibility
                Ok(AvroValue::String(date.format("%Y-%m-%d").to_string()))
            }
            FieldValue::Decimal(decimal) => {
                // Convert rust_decimal to Avro string representation
                Ok(AvroValue::String(decimal.to_string()))
            }
            FieldValue::Struct(map) => {
                // Convert Struct to Record - this requires a schema which we don't have
                // So convert to Map for now
                let mut avro_map = HashMap::new();
                for (k, v) in map {
                    let avro_value = self.field_value_to_avro(v)?;
                    avro_map.insert(k.clone(), avro_value);
                }
                Ok(AvroValue::Map(avro_map))
            }
            FieldValue::Interval { value, unit } => {
                // Convert time interval to string representation
                Ok(AvroValue::String(format!("{} {:?}", value, unit)))
            }
        }
    }

    /// Convert Avro Value to HashMap<String, FieldValue>
    fn avro_value_to_record(
        &self,
        avro_value: &AvroValue,
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        match avro_value {
            AvroValue::Record(fields) => {
                let mut record = HashMap::new();
                for (key, value) in fields {
                    let field_value =
                        self.avro_value_to_field_value_with_context(value, Some(key))?;
                    record.insert(key.clone(), field_value);
                }
                Ok(record)
            }
            _ => Err(SerializationError::SchemaError(
                "Expected Avro record, got other type".to_string(),
            )),
        }
    }

    /// Convert Avro Value to FieldValue with field context for logical type detection
    fn avro_value_to_field_value_with_context(
        &self,
        avro_value: &AvroValue,
        field_name: Option<&str>,
    ) -> Result<FieldValue, SerializationError> {
        match avro_value {
            AvroValue::Null => Ok(FieldValue::Null),
            AvroValue::Boolean(b) => Ok(FieldValue::Boolean(*b)),
            AvroValue::Int(i) => Ok(FieldValue::Integer(*i as i64)),
            AvroValue::Long(l) => Ok(FieldValue::Integer(*l)),
            AvroValue::Float(f) => {
                // Check if this field should be treated as a decimal logical type
                if let Some(name) = field_name {
                    if let Some(scale) = self.get_decimal_scale_from_schema(name) {
                        let scaled_value = (*f as f64 * 10_f64.powf(scale as f64)).round() as i64;
                        return Ok(FieldValue::ScaledInteger(scaled_value, scale));
                    }
                }
                Ok(FieldValue::Float(*f as f64))
            }
            AvroValue::Double(d) => {
                // Check if this field should be treated as a decimal logical type
                if let Some(name) = field_name {
                    if let Some(scale) = self.get_decimal_scale_from_schema(name) {
                        let scaled_value = (*d * 10_f64.powf(scale as f64)).round() as i64;
                        return Ok(FieldValue::ScaledInteger(scaled_value, scale));
                    }
                }
                Ok(FieldValue::Float(*d))
            }
            AvroValue::String(s) => Ok(FieldValue::String(s.clone())),
            AvroValue::Bytes(bytes) => {
                // Check if this is a decimal logical type encoded as bytes
                if let Some(name) = field_name {
                    if let Some(scale) = self.get_decimal_scale_from_schema(name) {
                        // Try to decode as fixed-length big-endian integer
                        if bytes.len() <= 8 {
                            let mut padded = vec![0u8; 8];
                            let start = 8 - bytes.len();
                            padded[start..].copy_from_slice(bytes);
                            let scaled_value = i64::from_be_bytes(padded.try_into().unwrap());
                            return Ok(FieldValue::ScaledInteger(scaled_value, scale));
                        }
                    }
                }

                // Otherwise, try to decode as ScaledInteger if it looks like decimal bytes
                if bytes.len() == 8 {
                    let value = i64::from_be_bytes(bytes.as_slice().try_into().unwrap());
                    let scale = 2; // Default scale for unknown decimal bytes
                    Ok(FieldValue::ScaledInteger(value, scale))
                } else {
                    // Convert to base64 string
                    use base64::Engine;
                    let b64_str = base64::engine::general_purpose::STANDARD.encode(bytes);
                    Ok(FieldValue::String(b64_str))
                }
            }
            AvroValue::Array(arr) => {
                let mut field_values = Vec::new();
                for item in arr {
                    field_values.push(self.avro_value_to_field_value_with_context(item, None)?);
                }
                Ok(FieldValue::Array(field_values))
            }
            AvroValue::Map(map) => {
                let mut field_map = HashMap::new();
                for (k, v) in map {
                    // Convert Avro Value back to FieldValue
                    let field_value = self.avro_value_to_field_value_with_context(v, None)?;
                    field_map.insert(k.clone(), field_value);
                }
                Ok(FieldValue::Map(field_map))
            }
            AvroValue::Union(_index, boxed_value) => {
                // Handle union types - typically used for nullable fields
                self.avro_value_to_field_value_with_context(boxed_value, field_name)
            }
            AvroValue::Record(fields) => {
                // Nested record - convert to structured Map
                let mut nested_map = HashMap::new();
                for (key, value) in fields {
                    let field_value =
                        self.avro_value_to_field_value_with_context(value, Some(key))?;
                    nested_map.insert(key.clone(), field_value);
                }
                Ok(FieldValue::Map(nested_map))
            }
            AvroValue::Enum(_index, symbol) => Ok(FieldValue::String(symbol.clone())),
            AvroValue::Fixed(_size, bytes) => {
                // Check if this is a decimal logical type
                if let Some(name) = field_name {
                    if let Some(scale) = self.get_decimal_scale_from_schema(name) {
                        if bytes.len() <= 8 {
                            let mut padded = vec![0u8; 8];
                            let start = 8 - bytes.len();
                            padded[start..].copy_from_slice(bytes);
                            let scaled_value = i64::from_be_bytes(padded.try_into().unwrap());
                            return Ok(FieldValue::ScaledInteger(scaled_value, scale));
                        }
                    }
                }

                // Convert fixed bytes to base64 string
                use base64::Engine;
                let b64_str = base64::engine::general_purpose::STANDARD.encode(bytes);
                Ok(FieldValue::String(b64_str))
            }
            _ => Err(SerializationError::SchemaError(format!(
                "Unsupported Avro value type: {:?}",
                avro_value
            ))),
        }
    }

    /// Convert Avro Value to FieldValue (backwards compatibility)
    fn avro_value_to_field_value(
        &self,
        avro_value: &AvroValue,
    ) -> Result<FieldValue, SerializationError> {
        self.avro_value_to_field_value_with_context(avro_value, None)
    }

    /// Get the decimal scale from the Avro schema for a specific field
    fn get_decimal_scale_from_schema(&self, field_name: &str) -> Option<u8> {
        // Parse the schema to find decimal logical type definitions
        let schema_json = self.schema.canonical_form();
        if let Ok(schema_value) = serde_json::from_str::<serde_json::Value>(&schema_json) {
            if let Some(fields) = schema_value.get("fields").and_then(|f| f.as_array()) {
                for field in fields {
                    if let (Some(name), Some(field_type)) = (
                        field.get("name").and_then(|n| n.as_str()),
                        field.get("type"),
                    ) {
                        if name == field_name {
                            return self.extract_decimal_scale_from_type(field_type);
                        }
                    }
                }
            }
        }
        None
    }

    /// Extract decimal scale from an Avro type definition
    fn extract_decimal_scale_from_type(&self, type_def: &serde_json::Value) -> Option<u8> {
        // Handle union types (e.g., ["null", {"type": "bytes", "logicalType": "decimal"}])
        if let Some(union_array) = type_def.as_array() {
            for union_type in union_array {
                if let Some(scale) = self.extract_decimal_scale_from_type(union_type) {
                    return Some(scale);
                }
            }
        }

        // Handle direct type objects
        if let Some(type_obj) = type_def.as_object() {
            if let (Some(logical_type), Some(scale)) = (
                type_obj.get("logicalType").and_then(|lt| lt.as_str()),
                type_obj.get("scale").and_then(|s| s.as_u64()),
            ) {
                if logical_type == "decimal" && scale <= 255 {
                    return Some(scale as u8);
                }
            }
        }

        None
    }
}

/// Implement Serializer trait for AvroCodec to work with ferris_streams KafkaConsumer
impl Serializer<HashMap<String, FieldValue>> for AvroCodec {
    fn serialize(
        &self,
        value: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        self.serialize(value)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<HashMap<String, FieldValue>, SerializationError> {
        self.deserialize(bytes)
    }
}

/// Create an Avro serializer that can be used with KafkaConsumer
pub fn create_avro_serializer(schema_json: &str) -> Result<AvroCodec, SerializationError> {
    AvroCodec::new(schema_json)
}

/// Convenience function to serialize a HashMap<String, FieldValue> to Avro bytes
pub fn serialize_to_avro(
    record: &HashMap<String, FieldValue>,
    schema_json: &str,
) -> Result<Vec<u8>, SerializationError> {
    let codec = AvroCodec::new(schema_json)?;
    codec.serialize(record)
}

/// Convenience function to deserialize Avro bytes to HashMap<String, FieldValue>
pub fn deserialize_from_avro(
    bytes: &[u8],
    schema_json: &str,
) -> Result<HashMap<String, FieldValue>, SerializationError> {
    let codec = AvroCodec::new(schema_json)?;
    codec.deserialize(bytes)
}

/// Implementation of UnifiedCodec for runtime abstraction
impl serialization::traits::UnifiedCodec for AvroCodec {
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
        "Avro"
    }
}
