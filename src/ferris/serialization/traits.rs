//! Core serialization traits

use super::{FieldValue, SerializationError};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

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
pub trait SerializationFormat: Send + Sync {
    /// Serialize a record to bytes for Kafka production
    fn serialize_record(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError>;

    /// Deserialize bytes from Kafka into a record
    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError>;

    /// Get the format name (for logging/debugging)
    fn format_name(&self) -> &'static str;
}



/// Unified codec trait for all serialization formats
/// Enables runtime abstraction over different codec types
pub trait UnifiedCodec: Send + Sync {
    /// Serialize HashMap<String, FieldValue> to bytes
    fn serialize_record(&self, value: &HashMap<String, FieldValue>) -> Result<Vec<u8>, SerializationError>;

    /// Deserialize bytes to HashMap<String, FieldValue>
    fn deserialize_record(&self, bytes: &[u8]) -> Result<HashMap<String, FieldValue>, SerializationError>;

    /// Get the format name for debugging/logging
    fn format_name(&self) -> &'static str;
}
