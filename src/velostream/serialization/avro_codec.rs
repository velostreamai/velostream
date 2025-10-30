//! Avro codec for HashMap<String, FieldValue> serialization/deserialization

use crate::velostream::kafka::serialization::Serde;
use crate::velostream::serialization;
use crate::velostream::serialization::SerializationError;
use crate::velostream::sql::execution::types::FieldValue;
use apache_avro::{Reader, Schema as AvroSchema, Writer, types::Value as AvroValue};
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

        log::debug!("Serializing Avro record: {:?}", avro_value);

        // Use to_avro_datum for raw Avro encoding (NOT Object Container File format)
        // This is correct for Kafka messages - no embedded schema, just the data
        apache_avro::to_avro_datum(&self.schema, avro_value).map_err(|e| {
            log::error!("to_avro_datum() failed with error: {:?}", e);
            SerializationError::avro_error(&format!("Failed to encode Avro data: {}", e), e)
        })
    }

    /// Deserialize Avro bytes to HashMap<String, FieldValue>
    pub fn deserialize(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        // Use from_avro_datum for raw Avro decoding (NOT Object Container File format)
        // This matches the to_avro_datum encoding used in serialize()
        let avro_value = apache_avro::from_avro_datum(&self.schema, &mut &bytes[..], None)
            .map_err(|e| SerializationError::avro_error("Failed to decode Avro datum", e))?;

        self.avro_value_to_record(&avro_value)
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
            // Pass field name for schema-aware conversion (especially for decimal types)
            let avro_value = self.field_value_to_avro_with_name(field_value, Some(key.as_str()))?;
            avro_fields.push((key.clone(), avro_value));
        }

        Ok(AvroValue::Record(avro_fields))
    }

    /// Convert FieldValue to Avro Value with schema-aware field name context
    fn field_value_to_avro_with_name(
        &self,
        field_value: &FieldValue,
        field_name: Option<&str>,
    ) -> Result<AvroValue, SerializationError> {
        // Check if this field is a decimal type in the schema
        let decimal_scale = field_name.and_then(|name| self.get_decimal_scale_from_schema(name));

        // Check for logical type in schema
        let logical_type = field_name.and_then(|name| self.get_logical_type_from_schema(name));

        log::debug!(
            "Converting field {:?}: value={:?}, decimal_scale={:?}, logical_type={:?}",
            field_name,
            field_value,
            decimal_scale,
            logical_type
        );

        match field_value {
            FieldValue::Null => Ok(AvroValue::Null),
            FieldValue::Boolean(b) => Ok(AvroValue::Boolean(*b)),
            FieldValue::Integer(i) if decimal_scale.is_some() => {
                // Convert Integer to Decimal when schema expects it
                let scale = decimal_scale.unwrap();
                let scaled_value = i * 10_i64.pow(scale as u32);
                self.scaled_integer_to_decimal_bytes(scaled_value, scale)
            }
            FieldValue::Integer(i) if logical_type == Some("date") => {
                // Convert Integer to Date logical type (days since epoch)
                Ok(AvroValue::Date(*i as i32))
            }
            FieldValue::Integer(i) if logical_type == Some("time-millis") => {
                // Convert Integer to TimeMillis logical type
                Ok(AvroValue::TimeMillis(*i as i32))
            }
            FieldValue::Integer(i) if logical_type == Some("time-micros") => {
                // Convert Integer to TimeMicros logical type
                Ok(AvroValue::TimeMicros(*i))
            }
            FieldValue::Integer(i) if logical_type == Some("timestamp-millis") => {
                // Convert Integer to TimestampMillis logical type
                Ok(AvroValue::TimestampMillis(*i))
            }
            FieldValue::Integer(i) if logical_type == Some("timestamp-micros") => {
                // Convert Integer to TimestampMicros logical type
                Ok(AvroValue::TimestampMicros(*i))
            }
            FieldValue::Integer(i) if self.schema_expects_int(field_name) => {
                // Convert Integer to Int when schema expects i32
                Ok(AvroValue::Int(*i as i32))
            }
            FieldValue::Integer(i) => Ok(AvroValue::Long(*i)),
            FieldValue::String(s) if logical_type == Some("uuid") => {
                // Keep as string for UUID (Avro UUID is stored as string)
                Ok(AvroValue::String(s.clone()))
            }
            FieldValue::Float(f) if decimal_scale.is_some() => {
                // Convert Float to Decimal when schema expects it
                let scale = decimal_scale.unwrap();
                let scaled_value = (f * 10_f64.powi(scale as i32)).round() as i64;
                let decimal_bytes = self.scaled_integer_to_decimal_bytes(scaled_value, scale)?;

                // Check if this field is part of a union (nullable)
                if let Some(name) = field_name {
                    if self.is_union_field(name) {
                        // Wrap in Union with variant index 1 (assuming [null, decimal] order)
                        return Ok(AvroValue::Union(1, Box::new(decimal_bytes)));
                    }
                }

                Ok(decimal_bytes)
            }
            FieldValue::Float(f) => Ok(AvroValue::Double(*f)),
            FieldValue::ScaledInteger(value, _scale) if decimal_scale.is_some() => {
                // Use schema scale, not the FieldValue scale
                let schema_scale = decimal_scale.unwrap();
                let decimal_bytes = self.scaled_integer_to_decimal_bytes(*value, schema_scale)?;

                // Check if this field is part of a union (nullable)
                if let Some(name) = field_name {
                    if self.is_union_field(name) {
                        // Wrap in Union with variant index 1 (assuming [null, decimal] order)
                        return Ok(AvroValue::Union(1, Box::new(decimal_bytes)));
                    }
                }

                Ok(decimal_bytes)
            }
            FieldValue::ScaledInteger(value, scale) => {
                // No decimal schema - convert to string representation
                let scale_factor = 10_i64.pow(*scale as u32);
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
                // Check if schema expects timestamp logical type
                if logical_type == Some("timestamp-millis") {
                    // Convert to milliseconds since epoch
                    let millis = ts.and_utc().timestamp_millis();
                    return Ok(AvroValue::TimestampMillis(millis));
                }
                if logical_type == Some("timestamp-micros") {
                    // Convert to microseconds since epoch
                    let micros = ts.and_utc().timestamp() * 1_000_000
                        + (ts.and_utc().timestamp_subsec_micros() as i64);
                    return Ok(AvroValue::TimestampMicros(micros));
                }
                // Fallback: Convert to ISO 8601 string format for schema compatibility
                Ok(AvroValue::String(
                    ts.format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
                ))
            }
            _ => self.field_value_to_avro_non_decimal(field_value),
        }
    }

    /// Convert FieldValue to Avro Value (non-schema-aware, for backwards compatibility)
    fn field_value_to_avro(
        &self,
        field_value: &FieldValue,
    ) -> Result<AvroValue, SerializationError> {
        self.field_value_to_avro_with_name(field_value, None)
    }

    /// Convert non-decimal FieldValue types to Avro Value
    fn field_value_to_avro_non_decimal(
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
                // Fallback: Convert to ISO 8601 string format when no schema context available
                // (When called with schema context, the case in field_value_to_avro_with_name handles it)
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
            AvroValue::Decimal(decimal) => {
                // Avro Decimal type - convert to ScaledInteger
                if let Some(name) = field_name {
                    if let Some(scale) = self.get_decimal_scale_from_schema(name) {
                        // Use TryFrom to get bytes from Decimal
                        let bytes: Vec<u8> = decimal.try_into().map_err(|e| {
                            SerializationError::SchemaError(format!(
                                "Failed to convert Decimal to bytes: {:?}",
                                e
                            ))
                        })?;

                        // Decode as big-endian signed integer
                        if bytes.len() <= 8 {
                            let mut padded = vec![0u8; 8];
                            let start = 8 - bytes.len();
                            padded[start..].copy_from_slice(&bytes);
                            let scaled_value = i64::from_be_bytes(padded.try_into().unwrap());
                            return Ok(FieldValue::ScaledInteger(scaled_value, scale));
                        }
                    }
                }

                // Fallback: convert to string
                Err(SerializationError::SchemaError(
                    "Decimal value without proper schema context or value too large".to_string(),
                ))
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
            AvroValue::TimestampMillis(millis) => {
                // Convert timestamp-millis (milliseconds since Unix epoch) to Timestamp
                use chrono::NaiveDateTime;
                let seconds = millis / 1000;
                let nanos = ((millis % 1000) * 1_000_000) as u32;
                #[allow(deprecated)]
                match NaiveDateTime::from_timestamp_opt(seconds, nanos) {
                    Some(dt) => Ok(FieldValue::Timestamp(dt)),
                    None => {
                        // If conversion fails, store as Integer
                        Ok(FieldValue::Integer(*millis))
                    }
                }
            }
            AvroValue::TimestampMicros(micros) => {
                // Convert timestamp-micros (microseconds since Unix epoch) to Timestamp
                use chrono::NaiveDateTime;
                let seconds = micros / 1_000_000;
                let nanos = ((micros % 1_000_000) * 1000) as u32;
                #[allow(deprecated)]
                match NaiveDateTime::from_timestamp_opt(seconds, nanos) {
                    Some(dt) => Ok(FieldValue::Timestamp(dt)),
                    None => {
                        // If conversion fails, store as Integer
                        Ok(FieldValue::Integer(*micros))
                    }
                }
            }
            AvroValue::Date(days) => {
                // Convert date (days since Unix epoch) to Date
                use chrono::{Duration, NaiveDate};
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                match epoch.checked_add_signed(Duration::days(*days as i64)) {
                    Some(date) => Ok(FieldValue::Date(date)),
                    None => {
                        // If conversion fails, store as Integer
                        Ok(FieldValue::Integer(*days as i64))
                    }
                }
            }
            AvroValue::TimeMillis(millis) => {
                // Time-millis is milliseconds since midnight - store as Integer
                Ok(FieldValue::Integer(*millis as i64))
            }
            AvroValue::TimeMicros(micros) => {
                // Time-micros is microseconds since midnight - store as Integer
                Ok(FieldValue::Integer(*micros))
            }
            AvroValue::Duration(duration) => {
                // Avro duration (months, days, millis) - convert to Interval
                use crate::velostream::sql::ast::TimeUnit;
                // Duration in Avro is 12 bytes: 4 bytes months, 4 bytes days, 4 bytes millis
                // For now, convert to total milliseconds and store as Interval
                // Note: Avro Duration has months, days, millis - we're simplifying to millis only
                let millis_u32: u32 = duration.millis().into();
                let total_millis = millis_u32 as i64;
                Ok(FieldValue::Interval {
                    value: total_millis,
                    unit: TimeUnit::Millisecond,
                })
            }
            AvroValue::Uuid(uuid) => {
                // Convert UUID to String
                Ok(FieldValue::String(uuid.to_string()))
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
        // IMPORTANT: Use canonical_form() which preserves precision/scale even though logicalType is stripped
        let schema_json = self.schema.canonical_form();

        log::debug!("Looking for decimal scale for field: {}", field_name);
        log::debug!("Canonical schema: {}", schema_json);

        if let Ok(schema_value) = serde_json::from_str::<serde_json::Value>(&schema_json) {
            if let Some(fields) = schema_value.get("fields").and_then(|f| f.as_array()) {
                for field in fields {
                    if let (Some(name), Some(field_type)) = (
                        field.get("name").and_then(|n| n.as_str()),
                        field.get("type"),
                    ) {
                        if name == field_name {
                            log::debug!("Found field '{}' with type: {:?}", name, field_type);
                            let scale = self.extract_decimal_scale_from_type(field_type);
                            log::debug!("Extracted scale for field '{}': {:?}", name, scale);
                            return scale;
                        }
                    }
                }
            }
        }
        log::debug!("No decimal scale found for field: {}", field_name);
        None
    }

    /// Get the logical type from the Avro schema for a specific field
    fn get_logical_type_from_schema(&self, field_name: &str) -> Option<&'static str> {
        // Get the schema for the specific field
        if let AvroSchema::Record(record_schema) = &self.schema {
            for field in &record_schema.fields {
                if field.name == field_name {
                    // Check the field's schema for logical types
                    return match &field.schema {
                        AvroSchema::Date => Some("date"),
                        AvroSchema::TimeMillis => Some("time-millis"),
                        AvroSchema::TimeMicros => Some("time-micros"),
                        AvroSchema::TimestampMillis => Some("timestamp-millis"),
                        AvroSchema::TimestampMicros => Some("timestamp-micros"),
                        AvroSchema::Uuid => Some("uuid"),
                        AvroSchema::Duration => Some("duration"),
                        _ => None,
                    };
                }
            }
        }
        None
    }

    /// Check if a field is a union type (nullable)
    fn is_union_field(&self, field_name: &str) -> bool {
        if let AvroSchema::Record(record_schema) = &self.schema {
            for field in &record_schema.fields {
                if field.name == field_name {
                    return matches!(field.schema, AvroSchema::Union(_));
                }
            }
        }
        false
    }

    /// Check if a field expects Int (i32) instead of Long (i64)
    fn schema_expects_int(&self, field_name: Option<&str>) -> bool {
        if let Some(name) = field_name {
            if let AvroSchema::Record(record_schema) = &self.schema {
                for field in &record_schema.fields {
                    if field.name == name {
                        return matches!(field.schema, AvroSchema::Int);
                    }
                }
            }
        }
        false
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
            // In canonical form, logicalType is stripped, so check for bytes/fixed type with precision/scale
            let has_precision = type_obj.get("precision").and_then(|p| p.as_u64()).is_some();
            let scale_value = type_obj.get("scale").and_then(|s| s.as_u64());
            let type_name = type_obj.get("type").and_then(|t| t.as_str());

            // Decimal is bytes or fixed type with precision and scale
            if has_precision
                && scale_value.is_some()
                && (type_name == Some("bytes") || type_name == Some("fixed"))
            {
                let scale = scale_value.unwrap();
                if scale <= 255 {
                    return Some(scale as u8);
                }
            }

            // Also check for explicit logicalType (in case schema isn't canonical)
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

    /// Convert scaled integer to Avro Decimal bytes (big-endian two's complement)
    fn scaled_integer_to_decimal_bytes(
        &self,
        value: i64,
        _scale: u8,
    ) -> Result<AvroValue, SerializationError> {
        use apache_avro::Decimal as AvroDecimal;

        // Convert i64 to bytes (big-endian two's complement)
        // Use minimal byte representation (trim leading zeros for positive, leading 0xff for negative)
        let mut bytes = value.to_be_bytes().to_vec();

        // Trim leading zeros for positive numbers, but keep at least one byte
        // and preserve the sign bit
        if value >= 0 {
            while bytes.len() > 1 && bytes[0] == 0 && (bytes[1] & 0x80) == 0 {
                bytes.remove(0);
            }
        } else {
            // For negative numbers, trim leading 0xFF bytes
            while bytes.len() > 1 && bytes[0] == 0xFF && (bytes[1] & 0x80) != 0 {
                bytes.remove(0);
            }
        }

        log::debug!(
            "Converting value {} to decimal bytes (trimmed): {:?}",
            value,
            bytes
        );

        // Create Avro Decimal from bytes
        let decimal = AvroDecimal::from(bytes);

        Ok(AvroValue::Decimal(decimal))
    }
}

/// Implement Serializer trait for AvroCodec to work with velo_streams KafkaConsumer
impl Serde<HashMap<String, FieldValue>> for AvroCodec {
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
