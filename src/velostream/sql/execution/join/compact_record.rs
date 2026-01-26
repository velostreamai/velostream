//! Compact Record Storage for Join State
//!
//! Provides memory-efficient storage for records in join state stores by
//! replacing HashMap<String, FieldValue> with schema-indexed Vec<FieldValue>.
//!
//! ## Memory Savings
//!
//! For a record with 10 fields:
//! - Before: HashMap overhead (~48 bytes) + 10 String allocations (~240 bytes)
//! - After: Vec<FieldValue> (~24 bytes) + shared schema reference (8 bytes)
//!
//! Expected savings: 15-25% memory reduction + better cache locality.

use std::collections::HashMap;
use std::sync::Arc;

use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Schema for compact records - maps field names to positions
///
/// Shared across all records with the same structure, eliminating
/// per-record field name storage.
#[derive(Debug, Clone)]
pub struct RecordSchema {
    /// Field name to index mapping for O(1) lookup
    name_to_index: HashMap<String, usize>,
    /// Index to field name mapping for reconstruction
    index_to_name: Vec<String>,
}

impl RecordSchema {
    /// Create a new schema from field names
    ///
    /// Field order is preserved for deterministic serialization.
    pub fn new(field_names: Vec<String>) -> Self {
        let name_to_index: HashMap<String, usize> = field_names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();

        Self {
            name_to_index,
            index_to_name: field_names,
        }
    }

    /// Create schema from a StreamRecord
    ///
    /// Extracts field names in sorted order for consistency.
    pub fn from_record(record: &StreamRecord) -> Self {
        let mut field_names: Vec<String> = record.fields.keys().cloned().collect();
        field_names.sort(); // Ensure consistent ordering
        Self::new(field_names)
    }

    /// Get the index for a field name
    pub fn get_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    /// Get the field name at an index
    pub fn get_name(&self, index: usize) -> Option<&str> {
        self.index_to_name.get(index).map(|s| s.as_str())
    }

    /// Get the number of fields in the schema
    pub fn field_count(&self) -> usize {
        self.index_to_name.len()
    }

    /// Get all field names in order
    pub fn field_names(&self) -> &[String] {
        &self.index_to_name
    }

    /// Check if schema contains a field
    pub fn contains(&self, name: &str) -> bool {
        self.name_to_index.contains_key(name)
    }

    /// Check if this schema is compatible with another (same fields)
    pub fn is_compatible(&self, other: &RecordSchema) -> bool {
        self.index_to_name == other.index_to_name
    }

    /// Estimate memory usage of this schema
    pub fn estimated_size(&self) -> usize {
        // HashMap overhead + strings + Vec overhead
        let hashmap_overhead = self.name_to_index.capacity()
            * (std::mem::size_of::<String>() + std::mem::size_of::<usize>());
        let string_bytes: usize = self.index_to_name.iter().map(|s| s.len()).sum();
        let vec_overhead = self.index_to_name.capacity() * std::mem::size_of::<String>();

        hashmap_overhead + string_bytes + vec_overhead + std::mem::size_of::<Self>()
    }
}

/// Memory-efficient record storage using schema-indexed values
///
/// Replaces HashMap<String, FieldValue> with Vec<FieldValue> where
/// the schema provides field name mapping.
#[derive(Debug, Clone)]
pub struct CompactRecord {
    /// Shared schema for field name resolution
    schema: Arc<RecordSchema>,
    /// Field values indexed by schema position
    values: Vec<FieldValue>,
    /// Record timestamp (milliseconds since epoch)
    pub timestamp: i64,
    /// Event time for join operations
    pub event_time: Option<i64>,
}

impl CompactRecord {
    /// Create a compact record from a StreamRecord
    ///
    /// The schema must be compatible with the record's fields.
    pub fn from_stream_record(record: &StreamRecord, schema: Arc<RecordSchema>) -> Self {
        let mut values = vec![FieldValue::Null; schema.field_count()];

        for (name, value) in &record.fields {
            if let Some(index) = schema.get_index(name) {
                values[index] = value.clone();
            }
        }

        let event_time = record.event_time.map(|dt| dt.timestamp_millis());

        Self {
            schema,
            values,
            timestamp: record.timestamp,
            event_time,
        }
    }

    /// Create a compact record with a new schema derived from the record
    pub fn from_stream_record_with_new_schema(record: &StreamRecord) -> Self {
        let schema = Arc::new(RecordSchema::from_record(record));
        Self::from_stream_record(record, schema)
    }

    /// Convert back to a StreamRecord
    pub fn to_stream_record(&self) -> StreamRecord {
        let mut fields = HashMap::with_capacity(self.values.len());

        for (index, value) in self.values.iter().enumerate() {
            if let Some(name) = self.schema.get_name(index) {
                fields.insert(name.to_string(), value.clone());
            }
        }

        let event_time = self
            .event_time
            .and_then(chrono::DateTime::from_timestamp_millis);

        StreamRecord {
            fields,
            timestamp: self.timestamp,
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
            event_time,
            topic: None,
            key: None,
        }
    }

    /// Get a field value by name
    pub fn get(&self, name: &str) -> Option<&FieldValue> {
        self.schema
            .get_index(name)
            .and_then(|index| self.values.get(index))
    }

    /// Get a field value by index
    pub fn get_by_index(&self, index: usize) -> Option<&FieldValue> {
        self.values.get(index)
    }

    /// Get the schema
    pub fn schema(&self) -> &Arc<RecordSchema> {
        &self.schema
    }

    /// Get the number of fields
    pub fn field_count(&self) -> usize {
        self.values.len()
    }

    /// Estimate memory usage of this record
    ///
    /// Note: Schema size is NOT included as it's shared.
    pub fn estimated_size(&self) -> usize {
        let arc_overhead = std::mem::size_of::<Arc<RecordSchema>>();
        let values_overhead = self.values.capacity() * std::mem::size_of::<FieldValue>();
        let values_content: usize = self
            .values
            .iter()
            .map(|v| estimate_field_value_size(v))
            .sum();

        arc_overhead + values_overhead + values_content + std::mem::size_of::<Self>()
    }
}

/// Estimate the memory size of a FieldValue
fn estimate_field_value_size(value: &FieldValue) -> usize {
    match value {
        FieldValue::Null => 0,
        FieldValue::Boolean(_) => 1,
        FieldValue::Integer(_) => 8,
        FieldValue::Float(_) => 8,
        FieldValue::ScaledInteger(_, _) => 9,
        FieldValue::String(s) => s.len() + std::mem::size_of::<String>(),
        FieldValue::Timestamp(_) => 12,
        FieldValue::Date(_) => 4,
        FieldValue::Decimal(_) => 16,
        FieldValue::Interval { .. } => 16,
        FieldValue::Array(arr) => {
            let content: usize = arr.iter().map(estimate_field_value_size).sum();
            content + std::mem::size_of::<Vec<FieldValue>>()
        }
        FieldValue::Map(map) | FieldValue::Struct(map) => {
            let content: usize = map
                .iter()
                .map(|(k, v)| k.len() + estimate_field_value_size(v))
                .sum();
            content + std::mem::size_of::<HashMap<String, FieldValue>>()
        }
    }
}

/// Statistics about compact record usage
#[derive(Debug, Clone, Default)]
pub struct CompactRecordStats {
    /// Number of records stored
    pub record_count: usize,
    /// Number of unique schemas
    pub schema_count: usize,
    /// Estimated total memory for records (excluding shared schemas)
    pub record_memory_bytes: usize,
    /// Estimated total memory for schemas
    pub schema_memory_bytes: usize,
    /// Estimated memory saved vs HashMap storage
    pub estimated_savings_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_schema_creation() {
        let schema = RecordSchema::new(vec![
            "id".to_string(),
            "name".to_string(),
            "value".to_string(),
        ]);

        assert_eq!(schema.field_count(), 3);
        assert_eq!(schema.get_index("id"), Some(0));
        assert_eq!(schema.get_index("name"), Some(1));
        assert_eq!(schema.get_index("value"), Some(2));
        assert_eq!(schema.get_index("missing"), None);

        assert_eq!(schema.get_name(0), Some("id"));
        assert_eq!(schema.get_name(1), Some("name"));
        assert_eq!(schema.get_name(2), Some("value"));
        assert_eq!(schema.get_name(3), None);
    }

    #[test]
    fn test_schema_from_record() {
        let mut fields = HashMap::new();
        fields.insert("z_field".to_string(), FieldValue::Integer(1));
        fields.insert("a_field".to_string(), FieldValue::Integer(2));
        fields.insert("m_field".to_string(), FieldValue::Integer(3));

        let record = StreamRecord::new(fields);
        let schema = RecordSchema::from_record(&record);

        // Fields should be sorted alphabetically
        assert_eq!(schema.field_names(), &["a_field", "m_field", "z_field"]);
        assert_eq!(schema.get_index("a_field"), Some(0));
        assert_eq!(schema.get_index("m_field"), Some(1));
        assert_eq!(schema.get_index("z_field"), Some(2));
    }

    #[test]
    fn test_compact_record_roundtrip() {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(42));
        fields.insert("name".to_string(), FieldValue::String("test".to_string()));
        fields.insert("value".to_string(), FieldValue::Float(3.15));

        let mut original = StreamRecord::new(fields);
        original.timestamp = 12345;

        let schema = Arc::new(RecordSchema::from_record(&original));
        let compact = CompactRecord::from_stream_record(&original, schema);

        // Verify compact record access
        assert_eq!(compact.get("id"), Some(&FieldValue::Integer(42)));
        assert_eq!(
            compact.get("name"),
            Some(&FieldValue::String("test".to_string()))
        );
        assert_eq!(compact.get("value"), Some(&FieldValue::Float(3.15)));
        assert_eq!(compact.timestamp, 12345);

        // Convert back and verify
        let restored = compact.to_stream_record();
        assert_eq!(restored.fields.get("id"), Some(&FieldValue::Integer(42)));
        assert_eq!(
            restored.fields.get("name"),
            Some(&FieldValue::String("test".to_string()))
        );
        assert_eq!(restored.fields.get("value"), Some(&FieldValue::Float(3.15)));
        assert_eq!(restored.timestamp, 12345);
    }

    #[test]
    fn test_shared_schema() {
        let schema = Arc::new(RecordSchema::new(vec![
            "id".to_string(),
            "value".to_string(),
        ]));

        // Create multiple records sharing the same schema
        let mut fields1 = HashMap::new();
        fields1.insert("id".to_string(), FieldValue::Integer(1));
        fields1.insert("value".to_string(), FieldValue::Float(1.0));
        let record1 = StreamRecord::new(fields1);
        let compact1 = CompactRecord::from_stream_record(&record1, Arc::clone(&schema));

        let mut fields2 = HashMap::new();
        fields2.insert("id".to_string(), FieldValue::Integer(2));
        fields2.insert("value".to_string(), FieldValue::Float(2.0));
        let record2 = StreamRecord::new(fields2);
        let compact2 = CompactRecord::from_stream_record(&record2, Arc::clone(&schema));

        // Both use the same schema
        assert!(Arc::ptr_eq(&compact1.schema, &compact2.schema));

        // But have different values
        assert_eq!(compact1.get("id"), Some(&FieldValue::Integer(1)));
        assert_eq!(compact2.get("id"), Some(&FieldValue::Integer(2)));
    }

    #[test]
    fn test_schema_compatibility() {
        let schema1 = RecordSchema::new(vec!["a".to_string(), "b".to_string()]);
        let schema2 = RecordSchema::new(vec!["a".to_string(), "b".to_string()]);
        let schema3 = RecordSchema::new(vec!["b".to_string(), "a".to_string()]);
        let schema4 = RecordSchema::new(vec!["a".to_string(), "c".to_string()]);

        assert!(schema1.is_compatible(&schema2));
        assert!(!schema1.is_compatible(&schema3)); // Different order
        assert!(!schema1.is_compatible(&schema4)); // Different fields
    }

    #[test]
    fn test_memory_estimation() {
        let schema = Arc::new(RecordSchema::new(vec![
            "id".to_string(),
            "name".to_string(),
        ]));

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(42));
        fields.insert(
            "name".to_string(),
            FieldValue::String("test_value".to_string()),
        );
        let record = StreamRecord::new(fields);

        let compact = CompactRecord::from_stream_record(&record, schema.clone());

        // Verify memory estimation works (values should be > 0)
        assert!(compact.estimated_size() > 0);
        assert!(schema.estimated_size() > 0);
    }

    #[test]
    fn test_get_by_index() {
        let schema = Arc::new(RecordSchema::new(vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string(),
        ]));

        let mut fields = HashMap::new();
        fields.insert("first".to_string(), FieldValue::Integer(1));
        fields.insert("second".to_string(), FieldValue::Integer(2));
        fields.insert("third".to_string(), FieldValue::Integer(3));
        let record = StreamRecord::new(fields);

        let compact = CompactRecord::from_stream_record(&record, schema);

        assert_eq!(compact.get_by_index(0), Some(&FieldValue::Integer(1)));
        assert_eq!(compact.get_by_index(1), Some(&FieldValue::Integer(2)));
        assert_eq!(compact.get_by_index(2), Some(&FieldValue::Integer(3)));
        assert_eq!(compact.get_by_index(3), None);
    }
}
