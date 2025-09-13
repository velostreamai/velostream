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
//! use ferrisstreams::ferris::serialization::{JsonFormat, SerializationFormat, FieldValue};
//! use std::collections::HashMap;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a format
//! let format = JsonFormat;
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
//! # Available Formats
//!
//! You can directly instantiate the formats you need:
//!
//! ```rust
//! use ferrisstreams::ferris::serialization::{JsonFormat, AvroFormat};
//!
//! // JSON format (always available)
//! let json_format = JsonFormat;
//!
//! // Avro format (requires custom schema)
//! let avro_format = AvroFormat::default_format().unwrap();
//! ```
//!
//! # Schema-based Formats
//!
//! For Avro, you can provide custom schemas:
//!
//! ```rust
//! # fn avro_example() -> Result<(), Box<dyn std::error::Error>> {
//! use ferrisstreams::ferris::serialization::AvroFormat;
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
//! let format = AvroFormat::new(schema)?;
//! # Ok(())
//! # }
//! ```

// Re-export from SQL module for convenience
pub use crate::ferris::sql::FieldValue;

// Core types and traits
mod error;
mod traits;
// Format implementations
mod json;
pub use json::JsonFormat;

mod avro;
pub use avro::AvroFormat;

pub mod avro_codec;

mod protobuf;

pub mod protobuf_codec;

pub mod json_codec;

// Utilities
pub mod helpers;

// Re-export public API
pub use error::SerializationError;
pub use traits::SerializationFormat;

// Re-export format implementations for examples and tests only
pub use avro_codec::AvroCodec;
pub use json_codec::JsonCodec;
pub use protobuf_codec::ProtobufCodec;

// Serialization codec enum for Kafka integration
use crate::ferris::kafka::serialization::Serializer;
use std::collections::HashMap;

/// Unified serialization codec enum that implements Serializer trait
/// This allows KafkaConsumer to work with any of our supported formats
pub enum SerializationCodec {
    Json(JsonCodec),
    Avro(AvroCodec),
    Protobuf(ProtobufCodec),
}

impl Serializer<HashMap<String, FieldValue>> for SerializationCodec {
    fn serialize(
        &self,
        value: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        match self {
            Self::Json(codec) => codec.serialize(value),
            Self::Avro(codec) => codec.serialize(value),
            Self::Protobuf(codec) => codec.serialize(value),
        }
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<HashMap<String, FieldValue>, SerializationError> {
        match self {
            Self::Json(codec) => codec.deserialize(bytes),
            Self::Avro(codec) => codec.deserialize(bytes),
            Self::Protobuf(codec) => codec.deserialize(bytes),
        }
    }
}

impl SerializationCodec {
    /// Get the format name for debugging/logging
    pub fn format_name(&self) -> &'static str {
        match self {
            Self::Json(_) => "JSON",
            Self::Avro(_) => "Avro",
            Self::Protobuf(_) => "Protobuf",
        }
    }
}

// Re-export conversion helpers (used by external modules like kafka reader/writer)
