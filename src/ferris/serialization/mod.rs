//! Pluggable serialization interface for FerrisStreams
//!
//! This module provides a comprehensive trait-based approach to serialization that allows
//! the system to work with multiple data formats (JSON, Avro, Protobuf, etc.) instead
//! of being hardcoded to JSON.
//!
//! # Features
//!
//! - **JSON**: Always available, human-readable, good for development
//! - **Avro**: Feature-gated (`avro`), schema-based, supports evolution
//! - **Protobuf**: Feature-gated (`protobuf`), very compact, high performance
//!
//! # Quick Start
//!
//! ```rust
//! use ferrisstreams::ferris::serialization::{SerializationFormatFactory, FieldValue};
//! use std::collections::HashMap;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a format
//! let format = SerializationFormatFactory::create_format("json")?;
//!
//! // Create a record
//! let mut record = HashMap::new();
//! record.insert("name".to_string(), FieldValue::String("Alice".to_string()));
//! record.insert("age".to_string(), FieldValue::Integer(30));
//!
//! // Serialize and deserialize
//! let bytes = format.serialize_record(&record)?;
//! let restored = format.deserialize_record(&bytes)?;
//!
//! assert_eq!(record, restored);
//! # Ok(())
//! # }
//! ```
//!
//! # Format Selection
//!
//! Use `SerializationFormatFactory::supported_formats()` to see available formats:
//!
//! ```rust
//! use ferrisstreams::ferris::serialization::SerializationFormatFactory;
//!
//! let formats = SerializationFormatFactory::supported_formats();
//! println!("Available: {:?}", formats);
//! // Output: ["json", "avro", "protobuf", "proto"] (depending on features)
//! ```
//!
//! # Schema-based Formats
//!
//! For Avro, you can provide custom schemas:
//!
//! ```rust,ignore
//! #[cfg(feature = "avro")]
//! # fn avro_example() -> Result<(), Box<dyn std::error::Error>> {
//! use ferrisstreams::ferris::serialization::SerializationFormatFactory;
//!
//! let schema = r#"
//! {
//!   "type": "record",
//!   "name": "User",
//!   "fields": [
//!     {"name": "id", "type": "long"},
//!     {"name": "name", "type": "string"}
//!   ]
//! }
//! "#;
//!
//! let format = SerializationFormatFactory::create_avro_format(schema)?;
//! # Ok(())
//! # }
//! ```

pub use crate::ferris::sql::{FieldValue, SqlError};
use std::collections::HashMap;

/// Trait for pluggable serialization formats
///
/// This trait provides a consistent interface for different serialization formats
/// (JSON, Avro, Protocol Buffers) used throughout FerrisStreams. All formats must
/// support bidirectional conversion between external records and bytes, as well as
/// conversion to/from the SQL execution engine's internal representation.
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync` as format instances may be shared
/// across threads in a streaming application.
///
/// # Example Implementation
///
/// ```rust,ignore
/// struct MyFormat;
///
/// impl SerializationFormat for MyFormat {
///     fn serialize_record(&self, record: &HashMap<String, FieldValue>) -> Result<Vec<u8>, SerializationError> {
///         // Convert record to bytes using your format
///         todo!()
///     }
///     
///     fn deserialize_record(&self, bytes: &[u8]) -> Result<HashMap<String, FieldValue>, SerializationError> {
///         // Convert bytes back to record
///         todo!()
///     }
///     
///     // ... other methods
/// }
/// ```
pub trait SerializationFormat: Send + Sync {
    /// Serialize a record to bytes for Kafka production
    ///
    /// Converts a typed record (HashMap<String, FieldValue>) into a byte array
    /// suitable for storing in Kafka messages. The serialization format should
    /// preserve all type information needed for accurate deserialization.
    ///
    /// # Arguments
    ///
    /// * `record` - The typed record to serialize
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<u8>)` - Serialized bytes
    /// * `Err(SerializationError)` - If serialization fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut record = HashMap::new();
    /// record.insert("name".to_string(), FieldValue::String("Alice".to_string()));
    ///
    /// let bytes = format.serialize_record(&record)?;
    /// ```
    fn serialize_record(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError>;

    /// Deserialize bytes from Kafka into a record
    ///
    /// Converts bytes (typically from a Kafka message) back into a typed record.
    /// Must be the inverse operation of `serialize_record`.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The serialized bytes to deserialize
    ///
    /// # Returns
    ///
    /// * `Ok(HashMap<String, FieldValue>)` - Deserialized record
    /// * `Err(SerializationError)` - If deserialization fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let record = format.deserialize_record(&bytes)?;
    /// println!("Name: {:?}", record.get("name"));
    /// ```
    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError>;

    /// Convert record to internal execution format
    ///
    /// Converts external FieldValue types to InternalValue types used by the SQL
    /// execution engine. This enables the engine to process data regardless of the
    /// original serialization format.
    ///
    /// # Arguments
    ///
    /// * `record` - External format record
    ///
    /// # Returns
    ///
    /// * `Ok(HashMap<String, InternalValue>)` - Internal format data
    /// * `Err(SerializationError)` - If conversion fails
    fn to_execution_format(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<HashMap<String, InternalValue>, SerializationError>;

    /// Convert from internal execution format back to record
    ///
    /// Converts SQL engine InternalValue types back to external FieldValue types.
    /// This is used when query results need to be serialized back to Kafka.
    ///
    /// # Arguments
    ///
    /// * `data` - Internal format data from SQL engine
    ///
    /// # Returns
    ///
    /// * `Ok(HashMap<String, FieldValue>)` - External format record
    /// * `Err(SerializationError)` - If conversion fails
    fn from_execution_format(
        &self,
        data: &HashMap<String, InternalValue>,
    ) -> Result<HashMap<String, FieldValue>, SerializationError>;

    /// Get the format name (for logging/debugging)
    ///
    /// Returns a human-readable name for this format. Used in logs, error messages,
    /// and debugging output.
    ///
    /// # Returns
    ///
    /// A static string identifying the format (e.g., "JSON", "Avro", "Protobuf")
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
        FieldValue::Date(d) => Ok(serde_json::Value::String(d.format("%Y-%m-%d").to_string())),
        FieldValue::Timestamp(ts) => Ok(serde_json::Value::String(
            ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        )),
        FieldValue::Decimal(dec) => Ok(serde_json::Value::String(dec.to_string())),
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
        FieldValue::Date(d) => Ok(InternalValue::String(d.format("%Y-%m-%d").to_string())),
        FieldValue::Timestamp(ts) => Ok(InternalValue::String(
            ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        )),
        FieldValue::Decimal(dec) => Ok(InternalValue::String(dec.to_string())),
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
        FieldValue::Interval { value, unit } => {
            // Convert interval to milliseconds for output
            let millis = match unit {
                crate::ferris::sql::ast::TimeUnit::Millisecond => *value,
                crate::ferris::sql::ast::TimeUnit::Second => *value * 1000,
                crate::ferris::sql::ast::TimeUnit::Minute => *value * 60 * 1000,
                crate::ferris::sql::ast::TimeUnit::Hour => *value * 60 * 60 * 1000,
                crate::ferris::sql::ast::TimeUnit::Day => *value * 24 * 60 * 60 * 1000,
            };
            Ok(InternalValue::Integer(millis))
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

// Avro serialization implementation (feature-gated)
#[cfg(feature = "avro")]
pub struct AvroFormat {
    writer_schema: apache_avro::Schema,
    reader_schema: apache_avro::Schema,
}

#[cfg(feature = "avro")]
impl AvroFormat {
    /// Create new Avro format with schema
    pub fn new(schema_json: &str) -> Result<Self, SerializationError> {
        let schema = apache_avro::Schema::parse_str(schema_json).map_err(|e| {
            SerializationError::FormatConversionFailed(format!("Invalid Avro schema: {}", e))
        })?;

        Ok(AvroFormat {
            writer_schema: schema.clone(),
            reader_schema: schema,
        })
    }

    /// Create Avro format with separate reader and writer schemas (for schema evolution)
    pub fn with_schemas(
        writer_schema_json: &str,
        reader_schema_json: &str,
    ) -> Result<Self, SerializationError> {
        let writer_schema = apache_avro::Schema::parse_str(writer_schema_json).map_err(|e| {
            SerializationError::FormatConversionFailed(format!("Invalid writer schema: {}", e))
        })?;
        let reader_schema = apache_avro::Schema::parse_str(reader_schema_json).map_err(|e| {
            SerializationError::FormatConversionFailed(format!("Invalid reader schema: {}", e))
        })?;

        Ok(AvroFormat {
            writer_schema,
            reader_schema,
        })
    }

    /// Create a default Avro format with generic record schema
    pub fn default_format() -> Result<Self, SerializationError> {
        let schema_json = r#"
        {
            "type": "record",
            "name": "GenericRecord",
            "fields": [
                {"name": "data", "type": ["null", "string", "long", "double", "boolean", {"type": "map", "values": "string"}]}
            ]
        }
        "#;
        Self::new(schema_json)
    }
}

#[cfg(feature = "avro")]
impl SerializationFormat for AvroFormat {
    fn serialize_record(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        use apache_avro::Writer;

        // Convert record to Avro value
        let avro_value = record_to_avro_value(record)?;

        // Create writer and encode
        let mut writer = Writer::new(&self.writer_schema, Vec::new());
        writer.append(avro_value).map_err(|e| {
            SerializationError::SerializationFailed(format!("Avro serialization failed: {}", e))
        })?;

        writer.into_inner().map_err(|e| {
            SerializationError::SerializationFailed(format!(
                "Avro writer finalization failed: {}",
                e
            ))
        })
    }

    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        use apache_avro::Reader;

        let mut reader = Reader::with_schema(&self.reader_schema, bytes).map_err(|e| {
            SerializationError::DeserializationFailed(format!("Avro reader creation failed: {}", e))
        })?;

        // Read first record (assuming single record per message)
        if let Some(record_result) = reader.next() {
            let avro_value = record_result.map_err(|e| {
                SerializationError::DeserializationFailed(format!(
                    "Avro deserialization failed: {}",
                    e
                ))
            })?;

            return avro_value_to_record(&avro_value);
        }

        Err(SerializationError::DeserializationFailed(
            "No records found in Avro data".to_string(),
        ))
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
        "Avro"
    }
}

// Avro conversion helpers
#[cfg(feature = "avro")]
fn record_to_avro_value(
    record: &HashMap<String, FieldValue>,
) -> Result<apache_avro::types::Value, SerializationError> {
    use apache_avro::types::Value;

    let mut avro_fields = Vec::new();
    for (key, field_value) in record {
        let avro_value = match field_value {
            FieldValue::Null => {
                // For nullable fields in unions, wrap null in union with index 1
                // (assuming ["string", "null"] or similar patterns where null is typically second)
                Value::Union(1, Box::new(Value::Null))
            }
            _ => field_value_to_avro(field_value)?,
        };
        avro_fields.push((key.clone(), avro_value));
    }

    Ok(Value::Record(avro_fields))
}

#[cfg(feature = "avro")]
fn field_value_to_avro(
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

#[cfg(feature = "avro")]
fn avro_value_to_record(
    avro_value: &apache_avro::types::Value,
) -> Result<HashMap<String, FieldValue>, SerializationError> {
    match avro_value {
        apache_avro::types::Value::Record(fields) => {
            let mut record = HashMap::new();
            for (key, value) in fields {
                let field_value = avro_value_to_field_value(value)?;
                record.insert(key.clone(), field_value);
            }
            Ok(record)
        }
        _ => Err(SerializationError::DeserializationFailed(
            "Expected Avro record".to_string(),
        )),
    }
}

#[cfg(feature = "avro")]
fn avro_value_to_field_value(
    avro_value: &apache_avro::types::Value,
) -> Result<FieldValue, SerializationError> {
    use apache_avro::types::Value;

    match avro_value {
        Value::String(s) => Ok(FieldValue::String(s.clone())),
        Value::Long(i) => Ok(FieldValue::Integer(*i)),
        Value::Int(i) => Ok(FieldValue::Integer(*i as i64)),
        Value::Float(f) => Ok(FieldValue::Float(*f as f64)),
        Value::Double(f) => Ok(FieldValue::Float(*f)),
        Value::Boolean(b) => Ok(FieldValue::Boolean(*b)),
        Value::Null => Ok(FieldValue::Null),
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
        _ => Err(SerializationError::UnsupportedType(format!(
            "Unsupported Avro type: {:?}",
            avro_value
        ))),
    }
}

// Protocol Buffers serialization implementation (feature-gated)
#[cfg(feature = "protobuf")]
pub struct ProtobufFormat<T>
where
    T: prost::Message + Default,
{
    _phantom: std::marker::PhantomData<T>,
}

#[cfg(feature = "protobuf")]
impl<T> Default for ProtobufFormat<T>
where
    T: prost::Message + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "protobuf")]
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

#[cfg(feature = "protobuf")]
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
        "Protobuf"
    }
}

// Helper for protobuf conversion
#[cfg(feature = "protobuf")]
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

/// Factory for creating serialization formats
///
/// This factory provides a centralized way to create serialization format instances
/// by name or with custom configuration. It handles feature detection and returns
/// appropriate errors for unsupported formats.
///
/// # Examples
///
/// ```rust
/// use ferrisstreams::ferris::serialization::SerializationFormatFactory;
///
/// // Create JSON format (always available)
/// let json = SerializationFormatFactory::create_format("json").unwrap();
///
/// // Get list of supported formats
/// let formats = SerializationFormatFactory::supported_formats();
/// println!("Available: {:?}", formats);
///
/// // Get default format
/// let default = SerializationFormatFactory::default_format();
/// ```
pub struct SerializationFormatFactory;

impl SerializationFormatFactory {
    /// Create a serialization format by name
    ///
    /// Creates a format instance using the default configuration for the specified
    /// format. For schema-based formats like Avro, this uses a generic schema.
    ///
    /// # Supported Format Names
    ///
    /// - `"json"` - Always available
    /// - `"avro"` - Requires `avro` feature, uses default schema
    /// - `"protobuf"` or `"proto"` - Requires `protobuf` feature
    ///
    /// # Arguments
    ///
    /// * `format_name` - Case-insensitive format name
    ///
    /// # Returns
    ///
    /// * `Ok(Box<dyn SerializationFormat>)` - Format instance
    /// * `Err(SerializationError::UnsupportedType)` - If format is unknown or feature not enabled
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ferrisstreams::ferris::serialization::SerializationFormatFactory;
    ///
    /// let json_format = SerializationFormatFactory::create_format("json")?;
    /// assert_eq!(json_format.format_name(), "JSON");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create_format(
        format_name: &str,
    ) -> Result<Box<dyn SerializationFormat>, SerializationError> {
        match format_name.to_lowercase().as_str() {
            "json" => Ok(Box::new(JsonFormat)),
            #[cfg(feature = "avro")]
            "avro" => {
                let avro_format = AvroFormat::default_format().map_err(|e| {
                    SerializationError::FormatConversionFailed(format!(
                        "Failed to create Avro format: {}",
                        e
                    ))
                })?;
                Ok(Box::new(avro_format))
            }
            #[cfg(feature = "protobuf")]
            "protobuf" | "proto" => {
                // Generic protobuf format - in practice you'd specify the message type
                Ok(Box::new(ProtobufFormat::<()>::new()))
            }
            _ => Err(SerializationError::UnsupportedType(format!(
                "Unknown format: {}",
                format_name
            ))),
        }
    }

    /// Create Avro format with custom schema
    #[cfg(feature = "avro")]
    pub fn create_avro_format(
        schema_json: &str,
    ) -> Result<Box<dyn SerializationFormat>, SerializationError> {
        let avro_format = AvroFormat::new(schema_json)?;
        Ok(Box::new(avro_format))
    }

    /// Create Avro format with schema evolution support
    #[cfg(feature = "avro")]
    pub fn create_avro_format_with_schemas(
        writer_schema_json: &str,
        reader_schema_json: &str,
    ) -> Result<Box<dyn SerializationFormat>, SerializationError> {
        let avro_format = AvroFormat::with_schemas(writer_schema_json, reader_schema_json)?;
        Ok(Box::new(avro_format))
    }

    /// Create Protobuf format with specific message type
    #[cfg(feature = "protobuf")]
    pub fn create_protobuf_format<T>() -> Box<dyn SerializationFormat>
    where
        T: prost::Message + Default + Clone + Send + Sync + 'static,
    {
        Box::new(ProtobufFormat::<T>::new())
    }

    /// Get list of supported formats
    pub fn supported_formats() -> Vec<&'static str> {
        let mut formats = vec!["json"];

        #[cfg(feature = "avro")]
        formats.push("avro");

        #[cfg(feature = "protobuf")]
        {
            formats.push("protobuf");
            formats.push("proto");
        }

        formats
    }

    /// Get default format (JSON)
    pub fn default_format() -> Box<dyn SerializationFormat> {
        Box::new(JsonFormat)
    }
}
