//! JSON codec for HashMap<String, FieldValue> serialization/deserialization
//!
//! Performance optimizations:
//! - **Serialize**: Uses direct serialization via FieldValue's Serialize impl (2x faster)
//! - **Deserialize**: Uses direct deserialization via FieldValue's Deserialize impl (2x faster)
//!
//! Both paths use sonic-rs SIMD acceleration for optimal throughput.

use crate::velostream::kafka::serialization::Serde;
use crate::velostream::serialization::SerializationError;
use crate::velostream::sql::execution::types::FieldValue;
use std::collections::HashMap;

/// JSON codec that serializes/deserializes HashMap<String, FieldValue>
/// This enables unified consumer types across all serialization formats
///
/// Performance: Uses direct serialization/deserialization (no intermediate serde_json::Value)
/// and sonic-rs SIMD acceleration for optimal throughput.
pub struct JsonCodec;

impl JsonCodec {
    /// Create a new JsonCodec
    pub fn new() -> Self {
        JsonCodec
    }
}

impl Serde<HashMap<String, FieldValue>> for JsonCodec {
    /// Serialize HashMap<String, FieldValue> to JSON bytes
    ///
    /// Uses direct serialization via FieldValue's Serialize impl with sonic-rs SIMD.
    fn serialize(
        &self,
        value: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        // Direct serialization with sonic-rs SIMD acceleration
        // FieldValue implements Serialize, so no intermediate Value needed
        sonic_rs::to_vec(value)
            .map_err(|e| SerializationError::json_error("Failed to serialize JSON", e))
    }

    /// Deserialize JSON bytes to HashMap<String, FieldValue>
    ///
    /// Uses direct deserialization via FieldValue's Deserialize impl with sonic-rs SIMD.
    /// This is ~2x faster than deserializing to serde_json::Value first.
    fn deserialize(&self, bytes: &[u8]) -> Result<HashMap<String, FieldValue>, SerializationError> {
        // Direct deserialization with sonic-rs SIMD acceleration
        // FieldValue implements Deserialize, so no intermediate Value needed
        //
        // This parses JSON directly into HashMap<String, FieldValue> in a single pass,
        // avoiding the overhead of:
        // 1. Creating serde_json::Value tree
        // 2. Walking the tree to convert each Value to FieldValue
        // 3. Cloning strings from Value to FieldValue
        sonic_rs::from_slice(bytes)
            .map_err(|e| SerializationError::json_error("Failed to parse JSON", e))
    }
}

impl Default for JsonCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// Implementation of UnifiedCodec for runtime abstraction
impl crate::velostream::serialization::traits::UnifiedCodec for JsonCodec {
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
        "JSON"
    }
}
