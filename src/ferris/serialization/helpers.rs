//! Helper functions for serialization conversions

use super::{FieldValue, SerializationError};
use std::collections::HashMap;

// JSON conversion helpers

/// Convert JSON value to FieldValue
pub fn json_to_field_value(
    json_value: &serde_json::Value,
) -> Result<FieldValue, SerializationError> {
    match json_value {
        serde_json::Value::String(s) => {
            // Try to parse as decimal string (ScaledInteger)
            if let Some(decimal_pos) = s.find('.') {
                // This looks like a decimal string - try to parse as ScaledInteger
                let before_decimal = &s[..decimal_pos];
                let after_decimal = &s[decimal_pos + 1..];

                // Validate it's all digits
                if before_decimal
                    .chars()
                    .all(|c| c.is_ascii_digit() || c == '-')
                    && after_decimal.chars().all(|c| c.is_ascii_digit())
                {
                    let scale = after_decimal.len() as u8;
                    let scaled_value = format!("{}{}", before_decimal, after_decimal);

                    if let Ok(value) = scaled_value.parse::<i64>() {
                        return Ok(FieldValue::ScaledInteger(value, scale));
                    }
                }
            }

            // Not a decimal format, treat as regular string
            Ok(FieldValue::String(s.clone()))
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(FieldValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                // Keep as Float for general numeric values - ScaledInteger should be
                // used explicitly in financial contexts via proper type conversion
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

/// Convert FieldValue to JSON value
pub fn field_value_to_json(
    field_value: &FieldValue,
) -> Result<serde_json::Value, SerializationError> {
    match field_value {
        FieldValue::String(s) => Ok(serde_json::Value::String(s.clone())),
        FieldValue::Integer(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
        FieldValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                SerializationError::type_conversion_error(
                    format!("Invalid float: {}", f),
                    "String",
                    "Float",
                    None::<std::io::Error>,
                )
            }),
        FieldValue::Boolean(b) => Ok(serde_json::Value::Bool(*b)),
        FieldValue::Null => Ok(serde_json::Value::Null),
        FieldValue::Date(d) => Ok(serde_json::Value::String(d.format("%Y-%m-%d").to_string())),
        FieldValue::Timestamp(ts) => Ok(serde_json::Value::String(
            ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        )),
        FieldValue::Decimal(dec) => Ok(serde_json::Value::String(dec.to_string())),
        FieldValue::ScaledInteger(value, scale) => {
            // Serialize as standard decimal string for cross-system compatibility
            let divisor = 10_i64.pow(*scale as u32);
            let integer_part = value / divisor;
            let fractional_part = (value % divisor).abs();

            // CRITICAL: For financial precision, preserve ALL digits including trailing zeros
            // The scale is semantically important and must be preserved for round-trip compatibility
            let decimal_str = if *scale == 0 {
                // For scale 0, just use integer format
                integer_part.to_string()
            } else {
                // Always format with full precision - DO NOT trim trailing zeros
                // ScaledInteger(125000, 3) must serialize as "125.000", not "125.0"
                format!(
                    "{}.{:0width$}",
                    integer_part,
                    fractional_part,
                    width = *scale as usize
                )
            };
            Ok(serde_json::Value::String(decimal_str))
        }
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
        FieldValue::Interval { value, unit } => {
            let mut interval_obj = serde_json::Map::new();
            interval_obj.insert(
                "value".to_string(),
                serde_json::Value::Number(serde_json::Number::from(*value)),
            );
            interval_obj.insert(
                "unit".to_string(),
                serde_json::Value::String(format!("{:?}", unit)),
            );
            Ok(serde_json::Value::Object(interval_obj))
        }
    }
}

// Avro conversion helpers (feature-gated)

/// Convert Avro value to FieldValue
pub fn avro_value_to_field_value(
    avro_value: &apache_avro::types::Value,
) -> Result<FieldValue, SerializationError> {
    use apache_avro::types::Value;

    match avro_value {
        Value::String(s) => {
            // Try to detect if this string represents a financial decimal
            // This preserves ScaledInteger precision for financial data
            if let Some(scaled_integer) = parse_decimal_string_to_scaled_integer(s) {
                Ok(scaled_integer)
            } else {
                Ok(FieldValue::String(s.clone()))
            }
        }
        Value::Long(i) => Ok(FieldValue::Integer(*i)),
        Value::Int(i) => Ok(FieldValue::Integer(*i as i64)),
        Value::Float(f) => {
            // Keep as Float for general numeric values - ScaledInteger should be
            // used explicitly in financial contexts via proper type conversion
            Ok(FieldValue::Float(*f as f64))
        }
        Value::Double(f) => {
            // Keep as Float for general numeric values - ScaledInteger should be
            // used explicitly in financial contexts via proper type conversion
            Ok(FieldValue::Float(*f))
        }
        Value::Boolean(b) => Ok(FieldValue::Boolean(*b)),
        Value::Null => Ok(FieldValue::Null),
        Value::Bytes(bytes) => {
            // Handle Avro decimal logical type (bytes encoding)
            // Try to decode as decimal logical type first, fall back to generic bytes
            if let Some(scaled_integer) = try_decode_avro_decimal_bytes(bytes) {
                Ok(scaled_integer)
            } else {
                // Not a decimal logical type - handle as generic bytes
                // For now, convert to string representation for compatibility
                Ok(FieldValue::String(
                    bytes
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>(),
                ))
            }
        }
        Value::Fixed(_size, bytes) => {
            // Handle Avro decimal logical type (fixed encoding)
            // Try to decode as decimal logical type first, fall back to generic fixed
            if let Some(scaled_integer) = try_decode_avro_decimal_bytes(bytes) {
                Ok(scaled_integer)
            } else {
                // Not a decimal logical type - handle as generic fixed bytes
                Ok(FieldValue::String(
                    bytes
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>(),
                ))
            }
        }
        Value::Array(arr) => {
            let field_arr: Result<Vec<_>, _> = arr.iter().map(avro_value_to_field_value).collect();
            Ok(FieldValue::Array(field_arr?))
        }
        Value::Map(map) => {
            let mut field_map = HashMap::new();
            for (k, v) in map {
                field_map.insert(k.clone(), avro_value_to_field_value(v)?);
            }
            Ok(FieldValue::Map(field_map))
        }
        Value::Record(fields) => {
            let mut field_map = HashMap::new();
            for (k, v) in fields {
                field_map.insert(k.clone(), avro_value_to_field_value(v)?);
            }
            Ok(FieldValue::Struct(field_map))
        }
        Value::Union(_, boxed_value) => avro_value_to_field_value(boxed_value),
        _ => Err(SerializationError::type_conversion_error(
            format!("Unsupported Avro type: {:?}", avro_value),
            "AvroValue",
            "FieldValue",
            None::<std::io::Error>,
        )),
    }
}

/// Convert FieldValue to Avro value
pub fn field_value_to_avro(
    field_value: &FieldValue,
) -> Result<apache_avro::types::Value, SerializationError> {
    use apache_avro::types::Value;

    match field_value {
        FieldValue::String(s) => Ok(Value::String(s.clone())),
        FieldValue::Integer(i) => Ok(Value::Long(*i)),
        FieldValue::Float(f) => Ok(Value::Double(*f)),
        FieldValue::Boolean(b) => Ok(Value::Boolean(*b)),
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Date(d) => Ok(Value::String(d.format("%Y-%m-%d").to_string())),
        FieldValue::Timestamp(ts) => Ok(Value::String(
            ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        )),
        FieldValue::Decimal(dec) => Ok(Value::String(dec.to_string())),
        FieldValue::ScaledInteger(value, scale) => {
            // For maximum compatibility, we use string representation
            // In a schema-aware system, this should encode as bytes for decimal logical type
            let divisor = 10_i64.pow(*scale as u32);
            let integer_part = value / divisor;
            let fractional_part = (value % divisor).abs();
            let decimal_str = if fractional_part == 0 {
                integer_part.to_string()
            } else {
                format!(
                    "{}.{:0width$}",
                    integer_part,
                    fractional_part,
                    width = *scale as usize
                )
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
            };
            Ok(Value::String(decimal_str))
        }
        FieldValue::Array(arr) => {
            let avro_arr: Result<Vec<_>, _> = arr.iter().map(field_value_to_avro).collect();
            Ok(Value::Array(avro_arr?))
        }
        FieldValue::Map(map) => {
            let mut avro_map = std::collections::HashMap::new();
            for (k, v) in map {
                avro_map.insert(k.clone(), field_value_to_avro(v)?);
            }
            Ok(Value::Map(avro_map))
        }
        FieldValue::Struct(fields) => {
            let mut avro_fields = Vec::new();
            for (k, v) in fields {
                avro_fields.push((k.clone(), field_value_to_avro(v)?));
            }
            Ok(Value::Record(avro_fields))
        }
        FieldValue::Interval { value, unit } => {
            Ok(Value::String(format!("INTERVAL {} {:?}", value, unit)))
        }
    }
}

/// Convert ScaledInteger to Avro decimal logical type
/// For Apache Avro 0.20.0+ with standard decimal logical type support
pub fn scaled_integer_to_avro_decimal_bytes(
    value: i64,
    scale: u8,
) -> Result<apache_avro::types::Value, SerializationError> {
    use apache_avro::types::Value;

    eprintln!(
        "DEBUG: Converting ScaledInteger({}, {}) to Avro decimal for logical type",
        value, scale
    );

    // For Apache Avro 0.20.0+, use Value::Decimal when schema is parsed as Schema::Decimal
    // This provides better compatibility with the standard decimal logical type
    let decimal = apache_avro::Decimal::from(value.to_be_bytes().to_vec());
    let avro_value = Value::Decimal(decimal);
    eprintln!("DEBUG: Created Avro value: Decimal(...) for standard logical type compatibility");

    Ok(avro_value)
}

/// Convert ScaledInteger to Avro bytes (for custom properties approach)
/// For cross-system compatibility (Flink, etc.), when using custom decimalPrecision/decimalScale
pub fn scaled_integer_to_avro_bytes_custom(
    value: i64,
    scale: u8,
) -> Result<apache_avro::types::Value, SerializationError> {
    use apache_avro::types::Value;

    eprintln!(
        "DEBUG: Converting ScaledInteger({}, {}) to Avro bytes for custom properties",
        value, scale
    );

    // Encode as big-endian two's complement bytes per Avro decimal specification
    let bytes = encode_big_endian_signed(value);
    eprintln!("DEBUG: Encoded as {} bytes: {:?}", bytes.len(), bytes);

    // Use Value::Bytes for custom properties approach (Flink compatibility)
    let avro_value = Value::Bytes(bytes);
    eprintln!("DEBUG: Created Avro value: Bytes(...) for cross-system compatibility");

    Ok(avro_value)
}

/// Schema-aware FieldValue to Avro conversion for decimal logical types
/// Use this when you have schema information indicating decimal logical type
pub fn field_value_to_avro_with_schema(
    field_value: &FieldValue,
    use_decimal_logical_type: bool,
) -> Result<apache_avro::types::Value, SerializationError> {
    match field_value {
        FieldValue::ScaledInteger(value, _scale) if use_decimal_logical_type => {
            // Use proper Avro decimal logical type encoding
            scaled_integer_to_avro_decimal_bytes(*value, *_scale)
        }
        _ => {
            // Fall back to standard conversion
            field_value_to_avro(field_value)
        }
    }
}

/// Enhanced schema-aware FieldValue to Avro conversion that distinguishes between standard and custom decimal types
/// Use this when you have full decimal schema information
pub fn field_value_to_avro_with_decimal_schema(
    field_value: &FieldValue,
    decimal_info: Option<&DecimalSchemaInfo>,
) -> Result<apache_avro::types::Value, SerializationError> {
    match field_value {
        FieldValue::ScaledInteger(value, _scale) if decimal_info.is_some() => {
            let info = decimal_info.unwrap();
            if info.is_standard_logical_type {
                // Use Value::Decimal for standard "logicalType": "decimal"
                scaled_integer_to_avro_decimal_bytes(*value, *_scale)
            } else {
                // Use Value::Bytes for custom properties (Flink compatibility)
                scaled_integer_to_avro_bytes_custom(*value, *_scale)
            }
        }
        _ => {
            // Fall back to standard conversion
            field_value_to_avro(field_value)
        }
    }
}

/// Schema-aware Avro to FieldValue conversion that respects decimal logical types
/// This function takes schema information to properly decode decimal fields
pub fn avro_value_to_field_value_with_schema(
    value: &apache_avro::types::Value,
    field_schema: Option<&DecimalSchemaInfo>,
) -> Result<FieldValue, SerializationError> {
    use apache_avro::types::Value;

    match value {
        Value::Bytes(bytes) if field_schema.is_some() => {
            let schema_info = field_schema.unwrap();
            // Use schema-aware decoding for decimal logical type
            decode_avro_decimal_bytes_with_schema(bytes, schema_info.precision, schema_info.scale)
        }
        Value::Fixed(_size, bytes) if field_schema.is_some() => {
            let schema_info = field_schema.unwrap();
            // Use schema-aware decoding for decimal logical type
            decode_avro_decimal_bytes_with_schema(bytes, schema_info.precision, schema_info.scale)
        }
        Value::Decimal(decimal) if field_schema.is_some() => {
            let schema_info = field_schema.unwrap();
            eprintln!(
                "DEBUG: Deserializing Value::Decimal with schema precision={}, scale={}",
                schema_info.precision, schema_info.scale
            );

            // Extract bytes from Apache Avro Decimal
            // In Apache Avro 0.20.0, use TryFrom<Decimal> for Vec<u8>
            let bytes: Vec<u8> = decimal.clone().try_into().map_err(|e| {
                SerializationError::type_conversion_error(
                    "Failed to extract bytes from Decimal".to_string(),
                    "Decimal",
                    "bytes",
                    Some(e),
                )
            })?;
            eprintln!("DEBUG: Extracted {} bytes from Decimal", bytes.len());

            // Use schema-aware decoding for decimal logical type
            decode_avro_decimal_bytes_with_schema(&bytes, schema_info.precision, schema_info.scale)
        }
        _ => {
            // Fall back to standard conversion
            avro_value_to_field_value(value)
        }
    }
}

/// Decimal schema information extracted from Avro schema
#[derive(Debug, Clone)]
pub struct DecimalSchemaInfo {
    pub precision: u32,
    pub scale: u32,
    pub is_standard_logical_type: bool, // true for standard "logicalType": "decimal", false for custom properties
}

/// Encode signed integer as big-endian bytes (two's complement)
fn encode_big_endian_signed(value: i64) -> Vec<u8> {
    // For Avro decimal logical type, we need to preserve the proper two's complement representation
    // We cannot simply remove leading bytes as it breaks sign interpretation

    // Convert to big-endian bytes
    let bytes = value.to_be_bytes();

    // For Avro decimal, find the minimal representation that preserves two's complement
    let significant_bytes = if value >= 0 {
        // For positive numbers, remove leading zeros but ensure high bit is 0
        let start = bytes
            .iter()
            .position(|&b| b != 0)
            .unwrap_or(bytes.len() - 1);
        let significant = &bytes[start..];

        // If the high bit is set, we need to add a 0x00 byte to preserve positive sign
        if (significant[0] & 0x80) != 0 {
            let mut result = vec![0x00];
            result.extend_from_slice(significant);
            result
        } else {
            significant.to_vec()
        }
    } else {
        // For negative numbers, remove leading 0xFF bytes but ensure sign is preserved
        let start = bytes
            .iter()
            .position(|&b| b != 0xFF)
            .unwrap_or(bytes.len() - 1);
        let significant = if start > 0 && (bytes[start] & 0x80) == 0 {
            // Need to keep one 0xFF byte to preserve the sign
            &bytes[start - 1..]
        } else {
            &bytes[start..]
        };
        significant.to_vec()
    };

    significant_bytes
}

// Protobuf conversion helpers (feature-gated)

/// Convert protobuf bytes to FieldValue (placeholder implementation)
pub fn protobuf_bytes_to_field_value(_bytes: &[u8]) -> Result<FieldValue, SerializationError> {
    // This is a placeholder - real implementation would depend on the specific protobuf schema
    Err(SerializationError::type_conversion_error(
        "Generic protobuf conversion not implemented".to_string(),
        "ProtobufBytes",
        "FieldValue",
        None::<std::io::Error>,
    ))
}

/// Helper function to parse decimal strings and convert to ScaledInteger
/// This preserves exact financial precision when deserializing from Avro/JSON
fn parse_decimal_string_to_scaled_integer(s: &str) -> Option<FieldValue> {
    // Check if string looks like a decimal number (contains only digits, optional minus, and one decimal point)
    if !s
        .chars()
        .all(|c| c.is_ascii_digit() || c == '.' || c == '-')
    {
        return None;
    }

    // Try to parse as decimal
    if let Some(decimal_pos) = s.rfind('.') {
        // Has decimal point - parse as ScaledInteger
        let (integer_part, fractional_part) = s.split_at(decimal_pos);
        let fractional_part = &fractional_part[1..]; // Remove the '.'

        if fractional_part.is_empty() {
            return None; // Invalid decimal format
        }

        // Parse integer and fractional parts
        if let (Ok(integer_val), Ok(fractional_val)) =
            (integer_part.parse::<i64>(), fractional_part.parse::<u64>())
        {
            let scale = fractional_part.len() as u8;
            let divisor = 10_i64.pow(scale as u32);
            let scaled_value = integer_val * divisor
                + if integer_val < 0 {
                    -(fractional_val as i64)
                } else {
                    fractional_val as i64
                };

            Some(FieldValue::ScaledInteger(scaled_value, scale))
        } else {
            None
        }
    } else {
        // No decimal point - could be a whole number, but we only convert obvious financial decimals
        // Let regular string parsing handle integers
        None
    }
}

/// Helper function to try decoding Avro bytes/fixed as decimal logical type
/// Returns ScaledInteger if bytes look like they could be a decimal, None otherwise
///
/// Note: This is a heuristic approach since we don't have schema information.
/// In a production system, this should use the actual Avro schema to determine
/// if bytes represent a decimal logical type with specific precision/scale.
fn try_decode_avro_decimal_bytes(bytes: &[u8]) -> Option<FieldValue> {
    // DEPRECATED: This is heuristic-based and hardcodes scale=2
    // For proper schema-aware decoding, use decode_avro_decimal_bytes_with_schema
    // Keeping this for backward compatibility with non-schema-aware code

    // 1. Reasonable byte length for financial decimals (1-16 bytes)
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }

    // 2. Try to decode as big-endian signed integer
    if let Some(unscaled_value) = decode_big_endian_signed(bytes) {
        // 3. Use a common financial scale (2 decimal places for currency)
        // WARNING: This hardcoded scale=2 may not match the actual schema scale!
        let scale = 2;

        // 4. Sanity check - value should be reasonable for financial data
        // (not too large - under $1 trillion)
        if unscaled_value.abs() < 100_000_000_000_000 {
            // 100 trillion cents = 1 trillion dollars
            Some(FieldValue::ScaledInteger(unscaled_value, scale))
        } else {
            None
        }
    } else {
        None
    }
}

/// Schema-aware Avro decimal decoding that reads precision/scale from the schema
/// This is the CORRECT way to handle Avro decimal logical types
pub fn decode_avro_decimal_bytes_with_schema(
    bytes: &[u8],
    precision: u32,
    scale: u32,
) -> Result<FieldValue, SerializationError> {
    if bytes.is_empty() {
        return Err(SerializationError::type_conversion_error(
            "Empty bytes for decimal value".to_string(),
            "bytes",
            "ScaledInteger",
            None::<std::io::Error>,
        ));
    }

    // Validate precision/scale parameters
    if scale > precision {
        return Err(SerializationError::schema_validation_error(
            format!(
                "Scale {} cannot be greater than precision {}",
                scale, precision
            ),
            None::<std::io::Error>,
        ));
    }

    if scale > 255 {
        return Err(SerializationError::type_conversion_error(
            format!("Scale {} exceeds maximum u8 value", scale),
            "u32",
            "u8",
            None::<std::io::Error>,
        ));
    }

    // Decode big-endian two's complement bytes to signed integer
    let unscaled_value = decode_big_endian_signed_extended(bytes).ok_or_else(|| {
        SerializationError::type_conversion_error(
            "Failed to decode decimal bytes as signed integer".to_string(),
            "bytes",
            "i64",
            None::<std::io::Error>,
        )
    })?;

    // Validate the decoded value fits within precision
    let max_unscaled_value = 10_i128.pow(precision) - 1;
    if (unscaled_value as i128).abs() > max_unscaled_value {
        return Err(SerializationError::schema_validation_error(
            format!(
                "Decimal value exceeds precision {}: {}",
                precision, unscaled_value
            ),
            None::<std::io::Error>,
        ));
    }

    // Convert to ScaledInteger with the correct scale from schema
    Ok(FieldValue::ScaledInteger(unscaled_value, scale as u8))
}

/// Extended decoder for larger Avro decimal byte arrays (up to 16 bytes)
/// Used by schema-aware decimal decoding
fn decode_big_endian_signed_extended(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }

    // For bytes arrays larger than 8 bytes, we need to check if the value
    // can fit in i64. For now, limit to 8 bytes to be safe.
    if bytes.len() > 8 {
        // Check if leading bytes are just sign extension
        let sign_byte = if (bytes[0] & 0x80) != 0 { 0xFF } else { 0x00 };
        for &byte in &bytes[0..bytes.len() - 8] {
            if byte != sign_byte {
                return None; // Value too large for i64
            }
        }
        // Use the last 8 bytes
        return decode_big_endian_signed(&bytes[bytes.len() - 8..]);
    }

    decode_big_endian_signed(bytes)
}

/// Decode big-endian signed integer from bytes (two's complement)
fn decode_big_endian_signed(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || bytes.len() > 8 {
        return None;
    }

    // Convert big-endian bytes to i64
    let mut result = 0i64;
    let is_negative = (bytes[0] & 0x80) != 0;

    // Build the absolute value
    for &byte in bytes {
        result = result.checked_shl(8)?;
        result = result.checked_add(byte as i64)?;
    }

    // Handle two's complement for negative numbers
    if is_negative && bytes.len() < 8 {
        // Sign extend for negative numbers
        let sign_extension = !0i64 << (bytes.len() * 8);
        result |= sign_extension;
    }

    Some(result)
}

// Codec creation helpers

/// Create Avro codec with schema (optimized single codec creation)
pub fn create_avro_codec(
    schema: Option<&str>,
) -> Result<
    crate::ferris::serialization::avro_codec::AvroCodec,
    Box<dyn std::error::Error + Send + Sync>,
> {
    if let Some(schema_json) = schema {
        Ok(crate::ferris::serialization::avro_codec::AvroCodec::new(
            schema_json,
        )?)
    } else {
        Err("Avro format requires a schema to be provided".into())
    }
}

/// Create Protobuf codec with schema (optimized single codec creation)
pub fn create_protobuf_codec(
    schema: Option<&str>,
) -> Result<crate::ferris::serialization::ProtobufCodec, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(proto_schema) = schema {
        // Extract message type from schema or use default
        let message_type =
            extract_message_type_from_schema(proto_schema).unwrap_or("Record".to_string());
        Ok(crate::ferris::serialization::ProtobufCodec::new(
            proto_schema,
            &message_type,
        )?)
    } else {
        Err("Protobuf format REQUIRES a schema (.proto definition) to be provided".into())
    }
}

/// Extract message type from Protobuf schema (matches KafkaDataWriter pattern)
pub fn extract_message_type_from_schema(schema: &str) -> Option<String> {
    // Simple pattern matching to find "message MessageName" without regex dependency
    for line in schema.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("message ") {
            if let Some(message_name) = trimmed
                .strip_prefix("message ")
                .and_then(|s| s.split_whitespace().next())
                .and_then(|s| s.strip_suffix(" {").or(Some(s)))
            {
                return Some(message_name.to_string());
            }
        }
    }
    None
}
