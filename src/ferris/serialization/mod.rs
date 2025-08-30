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
//! ```rust
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

// Re-export from SQL module for convenience
pub use crate::ferris::sql::{FieldValue, SqlError};

// Core types and traits
mod error;
mod traits;
// Format implementations
mod json;

#[cfg(feature = "avro")]
mod avro;

#[cfg(feature = "protobuf")]
mod protobuf;

// Factory and utilities
mod factory;
pub mod helpers;

// Re-export public API
pub use error::SerializationError;
pub use factory::SerializationFormatFactory;
pub use traits::SerializationFormat;

// Re-export format implementations
pub use json::JsonFormat;

#[cfg(feature = "avro")]
pub use avro::AvroFormat;

#[cfg(feature = "protobuf")]
pub use protobuf::ProtobufFormat;

// Re-export conversion helpers (used by external modules like kafka reader/writer)
pub use helpers::{field_value_to_json, json_to_field_value};

#[cfg(feature = "avro")]
pub use helpers::{avro_value_to_field_value, field_value_to_avro};

#[cfg(feature = "protobuf")]
pub use helpers::protobuf_bytes_to_field_value;
