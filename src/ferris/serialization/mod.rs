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
pub use crate::ferris::sql::{FieldValue, SqlError};

// Core types and traits
mod error;
mod traits;
// Format implementations
mod json;

mod avro;

pub mod avro_codec;

mod protobuf;

pub mod protobuf_codec;

pub mod json_codec;

// Utilities
pub mod helpers;

// Re-export public API
pub use error::SerializationError;
pub use traits::SerializationFormat;


// Re-export format implementations
pub use json::JsonFormat;

pub use avro::AvroFormat;

pub use avro_codec::{
    create_avro_serializer, deserialize_from_avro, serialize_to_avro, AvroCodec
    ,
};

pub use protobuf::ProtobufFormat;

pub use protobuf_codec::{
    create_protobuf_serializer, deserialize_from_protobuf, serialize_to_protobuf, DecimalMessage,
    FieldMessage, ProtobufCodec, RecordMessage,
};

pub use json_codec::JsonCodec;

pub use traits::UnifiedCodec;

// Re-export conversion helpers (used by external modules like kafka reader/writer)
pub use helpers::{field_value_to_json, json_to_field_value};

pub use helpers::{avro_value_to_field_value, field_value_to_avro};

pub use helpers::protobuf_bytes_to_field_value;


