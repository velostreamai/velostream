//! Standardized property key constants for Kafka datasource configuration
//!
//! This module defines all standard property keys used across KafkaDataReader and KafkaDataWriter
//! to ensure consistent property naming and extraction. Using these constants prevents typos
//! and makes property key usage traceable across the codebase.

// ============================================================================
// FORMAT AND SERIALIZATION PROPERTIES
// ============================================================================

/// Primary format property key (e.g., "json", "avro", "protobuf")
pub const FORMAT: &str = "format";

/// Alternative format property key (with serializer prefix)
pub const SERIALIZER_FORMAT: &str = "serializer.format";

/// Format property key with value prefix
pub const VALUE_SERIALIZER: &str = "value.serializer";

/// Format property key with schema prefix
pub const SCHEMA_VALUE_SERIALIZER: &str = "schema.value.serializer";

/// All format property key variations (in preference order)
pub const FORMAT_KEYS: &[&str] = &[
    VALUE_SERIALIZER,        // "value.serializer" (Kafka convention)
    SCHEMA_VALUE_SERIALIZER, // "schema.value.serializer"
    SERIALIZER_FORMAT,       // "serializer.format"
    FORMAT,                  // "format" (fallback)
];

// ============================================================================
// SCHEMA PROPERTIES
// ============================================================================

/// Avro schema inline content property key
pub const AVRO_SCHEMA: &str = "avro.schema";

/// Avro schema file path property key
pub const AVRO_SCHEMA_FILE: &str = "avro.schema.file";

/// Alternative Avro schema property keys (in preference order)
pub const AVRO_SCHEMA_KEYS: &[&str] = &[
    AVRO_SCHEMA,         // "avro.schema"
    "value.avro.schema", // "value.avro.schema"
    "schema.avro",       // "schema.avro"
    "avro_schema",       // "avro_schema" (legacy underscore)
];

/// Alternative Avro schema file property keys (in preference order)
pub const AVRO_SCHEMA_FILE_KEYS: &[&str] = &[
    AVRO_SCHEMA_FILE,           // "avro.schema.file"
    "schema.value.schema.file", // "schema.value.schema.file"
    "value.schema.file",        // "value.schema.file"
    "schema.file",              // "schema.file"
    "avro_schema_file",         // "avro_schema_file" (legacy)
    "schema_file",              // "schema_file" (legacy)
];

/// Protobuf schema inline content property key
pub const PROTOBUF_SCHEMA: &str = "protobuf.schema";

/// Protobuf schema file path property key
pub const PROTOBUF_SCHEMA_FILE: &str = "protobuf.schema.file";

/// Alternative Protobuf schema property keys (in preference order)
pub const PROTOBUF_SCHEMA_KEYS: &[&str] = &[
    PROTOBUF_SCHEMA,         // "protobuf.schema"
    "value.protobuf.schema", // "value.protobuf.schema"
    "schema.protobuf",       // "schema.protobuf"
    "protobuf_schema",       // "protobuf_schema" (legacy)
    "proto.schema",          // "proto.schema"
];

/// Alternative Protobuf schema file property keys (in preference order)
pub const PROTOBUF_SCHEMA_FILE_KEYS: &[&str] = &[
    PROTOBUF_SCHEMA_FILE,   // "protobuf.schema.file"
    "proto.schema.file",    // "proto.schema.file"
    "value.schema.file",    // "value.schema.file"
    "schema.file",          // "schema.file"
    "protobuf_schema_file", // "protobuf_schema_file" (legacy)
    "schema_file",          // "schema_file" (legacy)
];

/// JSON schema inline content property key
pub const JSON_SCHEMA: &str = "json.schema";

/// Alternative JSON schema property keys (in preference order)
pub const JSON_SCHEMA_KEYS: &[&str] = &[
    JSON_SCHEMA,   // "json.schema"
    "schema.json", // "schema.json"
];

// ============================================================================
// KEY FIELD PROPERTIES
// ============================================================================

/// Message key field extraction property key
pub const KEY_FIELD: &str = "key.field";

/// Alternative key field property keys (in preference order)
pub const KEY_FIELD_KEYS: &[&str] = &[
    KEY_FIELD,           // "key.field"
    "message.key.field", // "message.key.field"
    "schema.key.field",  // "schema.key.field"
];

// ============================================================================
// TOPIC PROPERTIES
// ============================================================================

/// Topic name property key
pub const TOPIC: &str = "topic";

/// Topic name alternative property key (with name suffix)
pub const TOPIC_NAME: &str = "topic.name";

// ============================================================================
// CONSUMER PROPERTIES (Reader-specific)
// ============================================================================

/// Consumer group property key
pub const CONSUMER_GROUP: &str = "consumer.group";

/// Group ID property key (Kafka standard)
pub const GROUP_ID: &str = "group.id";

// ============================================================================
// METADATA/CONFIGURATION PROPERTIES
// ============================================================================

/// Performance profile property key (metadata only, not passed to Kafka)
pub const PERFORMANCE_PROFILE: &str = "performance_profile";

// ============================================================================
// PREFIX FILTERS (for property skipping/filtering)
// ============================================================================

/// Prefixes for properties that should be skipped (metadata/schema)
pub const SKIP_PREFIXES: &[&str] = &[
    "schema.",     // Schema configuration (handled separately)
    "value.",      // Value-specific config (Kafka will interpret these)
    "datasource.", // Datasource-specific config
    "avro.",       // Avro schema (handled separately)
    "protobuf.",   // Protobuf schema (handled separately)
    "proto.",      // Proto schema (handled separately)
    "json.",       // JSON schema (handled separately)
];

/// Exact property keys that should be skipped (metadata only)
pub const SKIP_EXACT: &[&str] = &[
    TOPIC,                   // Topic is not a producer property
    CONSUMER_GROUP,          // Consumer group is not a producer property
    KEY_FIELD,               // Key field extraction (handled separately)
    "message.key.field",     // Key field extraction (handled separately)
    "schema.key.field",      // Key field extraction (handled separately)
    PERFORMANCE_PROFILE,     // Performance profile is metadata
    FORMAT,                  // Format is handled separately
    SERIALIZER_FORMAT,       // Format is handled separately
    VALUE_SERIALIZER,        // Format is handled separately
    SCHEMA_VALUE_SERIALIZER, // Format is handled separately
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_keys_no_overlap_within_categories() {
        // Check for duplicates within each category (format, inline schemas, file schemas, etc.)
        // Note: Some keys like "schema.file" and "value.schema.file" are intentionally shared
        // between Avro and Protobuf as fallback/generic options.

        // Check FORMAT_KEYS
        let mut seen = std::collections::HashSet::new();
        for key in FORMAT_KEYS {
            assert!(seen.insert(key), "Duplicate key in FORMAT_KEYS: {}", key);
        }

        // Check inline schema keys
        seen.clear();
        for key in AVRO_SCHEMA_KEYS {
            assert!(
                seen.insert(key),
                "Duplicate key in AVRO_SCHEMA_KEYS: {}",
                key
            );
        }
        for key in PROTOBUF_SCHEMA_KEYS {
            assert!(
                seen.insert(key),
                "Duplicate key in PROTOBUF_SCHEMA_KEYS: {}",
                key
            );
        }
        for key in JSON_SCHEMA_KEYS {
            assert!(
                seen.insert(key),
                "Duplicate key in JSON_SCHEMA_KEYS: {}",
                key
            );
        }

        // Check file schema keys separately (they share fallback keys intentionally)
        seen.clear();
        for key in AVRO_SCHEMA_FILE_KEYS {
            assert!(
                seen.insert(key),
                "Duplicate key in AVRO_SCHEMA_FILE_KEYS: {}",
                key
            );
        }

        seen.clear();
        for key in PROTOBUF_SCHEMA_FILE_KEYS {
            assert!(
                seen.insert(key),
                "Duplicate key in PROTOBUF_SCHEMA_FILE_KEYS: {}",
                key
            );
        }

        // Check KEY_FIELD_KEYS
        seen.clear();
        for key in KEY_FIELD_KEYS {
            assert!(seen.insert(key), "Duplicate key in KEY_FIELD_KEYS: {}", key);
        }
    }

    #[test]
    fn test_format_keys_have_primary() {
        assert_eq!(FORMAT_KEYS.last(), Some(&FORMAT));
    }
}
