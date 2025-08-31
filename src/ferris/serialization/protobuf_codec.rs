//! Protobuf codec for HashMap<String, FieldValue> serialization/deserialization
//!
//! This codec provides direct protobuf serialization for FieldValue types, including
//! financial precision support through ScaledInteger types and proper decimal handling.

use crate::ferris::sql::execution::types::FieldValue;
use prost::Message;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

/// Error types for Protobuf codec operations
#[derive(Debug)]
pub enum ProtobufCodecError {
    SerializationError(String),
    DeserializationError(String),
    ConversionError(String),
    SchemaError(String),
}

impl fmt::Display for ProtobufCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtobufCodecError::SerializationError(msg) => {
                write!(f, "Serialization error: {}", msg)
            }
            ProtobufCodecError::DeserializationError(msg) => {
                write!(f, "Deserialization error: {}", msg)
            }
            ProtobufCodecError::ConversionError(msg) => write!(f, "Conversion error: {}", msg),
            ProtobufCodecError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
        }
    }
}

impl Error for ProtobufCodecError {}

/// Financial decimal message for precise monetary calculations
/// This follows the industry-standard approach for decimal representation in protobuf
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecimalMessage {
    /// The unscaled value as a 64-bit signed integer
    #[prost(int64, tag = "1")]
    pub units: i64,
    /// The number of decimal places (scale)
    #[prost(uint32, tag = "2")]
    pub scale: u32,
}

/// Generic field value message that can represent any FieldValue type
#[derive(Clone, PartialEq, ::prost::Oneof)]
pub enum FieldValueOneof {
    #[prost(string, tag = "1")]
    StringValue(String),
    #[prost(int64, tag = "2")]
    IntegerValue(i64),
    #[prost(double, tag = "3")]
    FloatValue(f64),
    #[prost(bool, tag = "4")]
    BooleanValue(bool),
    #[prost(message, tag = "5")]
    DecimalValue(DecimalMessage),
    #[prost(message, tag = "6")]
    TimestampValue(TimestampMessage),
    #[prost(message, tag = "7")]
    ArrayValue(ArrayMessage),
    #[prost(message, tag = "8")]
    MapValue(MapMessage),
}

/// Timestamp message
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimestampMessage {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(uint32, tag = "2")]
    nanos: u32,
}

/// Array message
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrayMessage {
    #[prost(message, repeated, tag = "1")]
    pub values: Vec<FieldMessage>,
}

/// Map message
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MapMessage {
    #[prost(map = "string, message", tag = "1")]
    pub entries: HashMap<String, FieldMessage>,
}

/// Main field message wrapper
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldMessage {
    #[prost(oneof = "FieldValueOneof", tags = "1,2,3,4,5,6,7,8")]
    pub value: Option<FieldValueOneof>,
}

/// Record message containing a map of named fields
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecordMessage {
    #[prost(map = "string, message", tag = "1")]
    pub fields: HashMap<String, FieldMessage>,
}

/// Protobuf codec for serializing/deserializing HashMap<String, FieldValue>
pub struct ProtobufCodec {
    /// Whether to use decimal precision for financial calculations
    use_financial_precision: bool,
}

impl Default for ProtobufCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtobufCodec {
    /// Create a new ProtobufCodec with default settings
    pub fn new() -> Self {
        ProtobufCodec {
            use_financial_precision: true,
        }
    }

    /// Create a new ProtobufCodec with financial precision control
    pub fn with_financial_precision(use_financial_precision: bool) -> Self {
        ProtobufCodec {
            use_financial_precision,
        }
    }

    /// Serialize a record to protobuf bytes
    pub fn serialize(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, ProtobufCodecError> {
        let mut proto_fields = HashMap::new();

        for (key, field_value) in record {
            let proto_field = self.field_value_to_proto(field_value)?;
            proto_fields.insert(key.clone(), proto_field);
        }

        let record_msg = RecordMessage {
            fields: proto_fields,
        };

        let mut buf = Vec::new();
        prost::Message::encode(&record_msg, &mut buf)
            .map_err(|e| ProtobufCodecError::SerializationError(e.to_string()))?;

        Ok(buf)
    }

    /// Deserialize protobuf bytes to a record
    pub fn deserialize(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, ProtobufCodecError> {
        let record_msg = RecordMessage::decode(bytes)
            .map_err(|e| ProtobufCodecError::DeserializationError(e.to_string()))?;

        let mut record = HashMap::new();
        for (key, proto_field) in record_msg.fields {
            let field_value = self.proto_to_field_value(&proto_field)?;
            record.insert(key, field_value);
        }

        Ok(record)
    }

    /// Convert FieldValue to protobuf message
    fn field_value_to_proto(
        &self,
        field_value: &FieldValue,
    ) -> Result<FieldMessage, ProtobufCodecError> {
        let proto_value = match field_value {
            FieldValue::String(s) => FieldValueOneof::StringValue(s.clone()),
            FieldValue::Integer(i) => FieldValueOneof::IntegerValue(*i),
            FieldValue::Float(f) => FieldValueOneof::FloatValue(*f),
            FieldValue::Boolean(b) => FieldValueOneof::BooleanValue(*b),
            FieldValue::ScaledInteger(value, scale) => {
                FieldValueOneof::DecimalValue(DecimalMessage {
                    units: *value,
                    scale: *scale as u32,
                })
            }
            FieldValue::Timestamp(ts) => {
                let timestamp_secs = ts.and_utc().timestamp();
                let timestamp_nanos = ts.and_utc().timestamp_subsec_nanos();
                FieldValueOneof::TimestampValue(TimestampMessage {
                    seconds: timestamp_secs,
                    nanos: timestamp_nanos,
                })
            }
            FieldValue::Array(arr) => {
                let mut proto_values = Vec::new();
                for item in arr {
                    proto_values.push(self.field_value_to_proto(item)?);
                }
                FieldValueOneof::ArrayValue(ArrayMessage {
                    values: proto_values,
                })
            }
            FieldValue::Map(map) => {
                let mut proto_entries = HashMap::new();
                for (k, v) in map {
                    proto_entries.insert(k.clone(), self.field_value_to_proto(v)?);
                }
                FieldValueOneof::MapValue(MapMessage {
                    entries: proto_entries,
                })
            }
            FieldValue::Struct(fields) => {
                // Convert struct to map representation
                let mut proto_entries = HashMap::new();
                for (k, v) in fields {
                    proto_entries.insert(k.clone(), self.field_value_to_proto(v)?);
                }
                FieldValueOneof::MapValue(MapMessage {
                    entries: proto_entries,
                })
            }
            FieldValue::Null => {
                // Protobuf doesn't have a null concept, so we'll use an empty string
                // In practice, you might use optional fields or wrapper types
                FieldValueOneof::StringValue(String::new())
            }
            FieldValue::Date(date) => {
                // Convert NaiveDate to string representation
                FieldValueOneof::StringValue(date.format("%Y-%m-%d").to_string())
            }
            FieldValue::Decimal(decimal) => {
                // Convert rust_decimal::Decimal to our DecimalMessage
                let scale = decimal.scale() as u32;
                let mantissa = decimal.mantissa();
                // Convert i128 to i64, handling potential overflow
                let units = if mantissa > i64::MAX as i128 || mantissa < i64::MIN as i128 {
                    // For very large decimals, convert to string representation
                    return Ok(FieldMessage {
                        value: Some(FieldValueOneof::StringValue(decimal.to_string())),
                    });
                } else {
                    mantissa as i64
                };
                FieldValueOneof::DecimalValue(DecimalMessage { units, scale })
            }
            FieldValue::Interval { value, unit } => {
                // Convert interval to string representation with value and unit
                let unit_str = format!("{:?}", unit); // Use Debug format for TimeUnit
                let interval_str = format!("{}_{}", value, unit_str);
                FieldValueOneof::StringValue(interval_str)
            }
        };

        Ok(FieldMessage {
            value: Some(proto_value),
        })
    }

    /// Convert protobuf message to FieldValue
    fn proto_to_field_value(
        &self,
        proto_field: &FieldMessage,
    ) -> Result<FieldValue, ProtobufCodecError> {
        match &proto_field.value {
            Some(FieldValueOneof::StringValue(s)) => {
                if s.is_empty() {
                    Ok(FieldValue::Null)
                } else {
                    Ok(FieldValue::String(s.clone()))
                }
            }
            Some(FieldValueOneof::IntegerValue(i)) => Ok(FieldValue::Integer(*i)),
            Some(FieldValueOneof::FloatValue(f)) => Ok(FieldValue::Float(*f)),
            Some(FieldValueOneof::BooleanValue(b)) => Ok(FieldValue::Boolean(*b)),
            Some(FieldValueOneof::DecimalValue(decimal)) => {
                if self.use_financial_precision {
                    Ok(FieldValue::ScaledInteger(
                        decimal.units,
                        decimal.scale as u8,
                    ))
                } else {
                    // Convert to float if financial precision is disabled
                    let scale_factor = 10_i64.pow(decimal.scale);
                    let float_value = decimal.units as f64 / scale_factor as f64;
                    Ok(FieldValue::Float(float_value))
                }
            }
            Some(FieldValueOneof::TimestampValue(ts)) => {
                use chrono::{DateTime, NaiveDateTime, Utc};
                let dt = DateTime::<Utc>::from_timestamp(ts.seconds, ts.nanos)
                    .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
                    .naive_utc();
                Ok(FieldValue::Timestamp(dt))
            }
            Some(FieldValueOneof::ArrayValue(arr)) => {
                let mut field_array = Vec::new();
                for proto_item in &arr.values {
                    field_array.push(self.proto_to_field_value(proto_item)?);
                }
                Ok(FieldValue::Array(field_array))
            }
            Some(FieldValueOneof::MapValue(map)) => {
                let mut field_map = HashMap::new();
                for (k, v) in &map.entries {
                    field_map.insert(k.clone(), self.proto_to_field_value(v)?);
                }
                Ok(FieldValue::Map(field_map))
            }
            None => Ok(FieldValue::Null),
        }
    }
}

/// Convenience function to serialize a record to protobuf bytes
pub fn serialize_to_protobuf(
    record: &HashMap<String, FieldValue>,
) -> Result<Vec<u8>, ProtobufCodecError> {
    let codec = ProtobufCodec::new();
    codec.serialize(record)
}

/// Convenience function to deserialize protobuf bytes to a record
pub fn deserialize_from_protobuf(
    bytes: &[u8],
) -> Result<HashMap<String, FieldValue>, ProtobufCodecError> {
    let codec = ProtobufCodec::new();
    codec.deserialize(bytes)
}

/// Create a protobuf serializer for Kafka integration
pub fn create_protobuf_serializer() -> ProtobufCodec {
    ProtobufCodec::new()
}
