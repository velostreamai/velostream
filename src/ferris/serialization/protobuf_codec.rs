//! Protobuf codec for HashMap<String, FieldValue> serialization/deserialization
//!
//! This codec provides direct protobuf serialization for FieldValue types, including
//! financial precision support through ScaledInteger types and proper decimal handling.

use crate::ferris::serialization::SerializationError;
use crate::ferris::sql::execution::types::FieldValue;
use crate::Serializer;
use prost::Message;
use std::collections::HashMap;

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
///
/// Note: This is a schema-less implementation using a generic message format.
/// For production use with specific schemas, consider using prost-build with .proto files.
pub struct ProtobufCodec {
    /// Schema definition (proto file content or descriptor)
    schema: String,
    /// Message type name from the schema to use for serialization
    message_type: String,
}

// Note: No Default implementation since Protobuf REQUIRES a schema
// Users must explicitly provide a schema when creating a ProtobufCodec

impl ProtobufCodec {
    /// Create a new ProtobufCodec with a proto schema
    ///
    /// # Arguments
    /// * `schema` - The protobuf schema definition (.proto file content)
    /// * `message_type` - The message type name from the schema to use
    ///
    /// # Example
    /// ```
    /// # use ferrisstreams::ferris::serialization::ProtobufCodec;
    /// let schema = r#"
    ///     syntax = "proto3";
    ///     message Record {
    ///         map<string, Field> fields = 1;
    ///     }
    ///     message Field {
    ///         oneof value {
    ///             string string_value = 1;
    ///             int64 integer_value = 2;
    ///             double float_value = 3;
    ///             bool boolean_value = 4;
    ///             Decimal decimal_value = 5;
    ///         }
    ///     }
    ///     message Decimal {
    ///         int64 units = 1;
    ///         uint32 scale = 2;
    ///     }
    /// "#;
    /// let codec = ProtobufCodec::new(schema, "Record").unwrap();
    /// ```
    pub fn new(schema: &str, message_type: &str) -> Result<Self, SerializationError> {
        // Validate that the schema is not empty
        if schema.trim().is_empty() {
            return Err(SerializationError::SchemaError(
                "Schema cannot be empty".to_string(),
            ));
        }

        if message_type.trim().is_empty() {
            return Err(SerializationError::SchemaError(
                "Message type name cannot be empty".to_string(),
            ));
        }

        Ok(ProtobufCodec {
            schema: schema.to_string(),
            message_type: message_type.to_string(),
        })
    }

    /// Create a new ProtobufCodec with the default generic schema
    /// This maintains backward compatibility with the existing implementation
    pub fn new_with_default_schema() -> Self {
        let default_schema = r#"
            syntax = "proto3";
            
            message RecordMessage {
                map<string, FieldMessage> fields = 1;
            }
            
            message FieldMessage {
                oneof value {
                    string string_value = 1;
                    int64 integer_value = 2;
                    double float_value = 3;
                    bool boolean_value = 4;
                    DecimalMessage decimal_value = 5;
                    TimestampMessage timestamp_value = 6;
                    ArrayMessage array_value = 7;
                    MapMessage map_value = 8;
                }
            }
            
            message DecimalMessage {
                int64 units = 1;
                uint32 scale = 2;
            }
            
            message TimestampMessage {
                int64 seconds = 1;
                uint32 nanos = 2;
            }
            
            message ArrayMessage {
                repeated FieldMessage values = 1;
            }
            
            message MapMessage {
                map<string, FieldMessage> entries = 1;
            }
        "#;

        ProtobufCodec {
            schema: default_schema.to_string(),
            message_type: "RecordMessage".to_string(),
        }
    }

    /// Load schema from a .proto file
    pub fn from_proto_file(proto_path: &str) -> Result<Self, SerializationError> {
        use std::fs;

        let schema = fs::read_to_string(proto_path).map_err(|e| {
            SerializationError::SchemaError(format!(
                "Failed to read proto file '{}': {}",
                proto_path, e
            ))
        })?;

        // Extract the main message type from the schema (simplified - in production use protoc)
        // For now, we'll require the user to specify the message type
        let message_type = "Record"; // Default assumption

        Self::new(&schema, message_type)
    }

    /// Get the schema definition
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Get the message type name
    pub fn message_type(&self) -> &str {
        &self.message_type
    }

    /// Serialize a record to protobuf bytes
    pub fn serialize(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
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
            .map_err(|e| SerializationError::SerializationFailed(e.to_string()))?;

        Ok(buf)
    }

    /// Deserialize protobuf bytes to a record
    pub fn deserialize(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        let record_msg = RecordMessage::decode(bytes)
            .map_err(|e| SerializationError::DeserializationFailed(e.to_string()))?;

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
    ) -> Result<FieldMessage, SerializationError> {
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
    ) -> Result<FieldValue, SerializationError> {
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
            Some(FieldValueOneof::DecimalValue(decimal)) => Ok(FieldValue::ScaledInteger(
                decimal.units,
                decimal.scale as u8,
            )),
            Some(FieldValueOneof::TimestampValue(ts)) => {
                use chrono::{DateTime, Utc};
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
/// Uses a default schema for basic Record message type
pub fn serialize_to_protobuf(
    record: &HashMap<String, FieldValue>,
) -> Result<Vec<u8>, SerializationError> {
    let default_schema = r#"
syntax = "proto3";

message Record {
    map<string, string> fields = 1;
}
"#;
    let codec = ProtobufCodec::new(default_schema, "Record")?;
    codec.serialize(record)
}

/// Convenience function to deserialize protobuf bytes to a record
/// Uses a default schema for basic Record message type
pub fn deserialize_from_protobuf(
    bytes: &[u8],
) -> Result<HashMap<String, FieldValue>, SerializationError> {
    let default_schema = r#"
syntax = "proto3";

message Record {
    map<string, string> fields = 1;
}
"#;
    let codec = ProtobufCodec::new(default_schema, "Record")?;
    codec.deserialize(bytes)
}

/// Create a protobuf serializer for Kafka integration
/// Create a protobuf serializer with required schema
/// Protobuf ALWAYS requires a schema to define message structure
pub fn create_protobuf_serializer(schema: &str) -> Result<ProtobufCodec, SerializationError> {
    // Try to parse the schema and extract message type
    // For simplicity, default to "Record" as the message type
    ProtobufCodec::new(schema, "Record")
}

/// Implement Serializer trait for ProtobufCodec to work with ferris_streams KafkaConsumer
impl Serializer<HashMap<String, FieldValue>> for ProtobufCodec {
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

/// Implementation of UnifiedCodec for runtime abstraction
impl crate::ferris::serialization::traits::UnifiedCodec for ProtobufCodec {
    fn serialize_record(
        &self,
        value: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        self.serialize(value)
            .map_err(|e| SerializationError::SerializationFailed(format!("{}", e)))
    }

    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        self.deserialize(bytes)
            .map_err(|e| SerializationError::DeserializationFailed(format!("{}", e)))
    }

    fn format_name(&self) -> &'static str {
        "Protobuf"
    }
}
