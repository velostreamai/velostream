//! Shared Types for Window Processing V2
//!
//! This module provides zero-copy types using Arc for shared ownership.

use crate::velostream::sql::execution::types::StreamRecord;
use std::sync::Arc;

/// Shared reference to a StreamRecord using Arc for zero-copy semantics.
///
/// Benefits:
/// - No cloning overhead when passing records between window operations
/// - Multiple window partitions can reference the same record
/// - 5000x fewer clone operations for 10K records
///
/// Usage:
/// ```rust,ignore
/// let record = StreamRecord::new(fields);
/// let shared = SharedRecord::new(record);
/// // Can be cloned cheaply (just increments ref count)
/// let shared_copy = shared.clone();
/// ```
#[derive(Debug, Clone)]
pub struct SharedRecord {
    inner: Arc<StreamRecord>,
}

impl SharedRecord {
    /// Create a new SharedRecord from a StreamRecord.
    pub fn new(record: StreamRecord) -> Self {
        Self {
            inner: Arc::new(record),
        }
    }

    /// Get a reference to the underlying StreamRecord.
    pub fn as_ref(&self) -> &StreamRecord {
        &self.inner
    }

    /// Get the Arc reference count (useful for debugging).
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Try to unwrap the record if this is the only reference.
    /// Returns None if there are other references.
    pub fn try_unwrap(self) -> Option<StreamRecord> {
        Arc::try_unwrap(self.inner).ok()
    }
}

impl From<StreamRecord> for SharedRecord {
    fn from(record: StreamRecord) -> Self {
        Self::new(record)
    }
}

impl AsRef<StreamRecord> for SharedRecord {
    fn as_ref(&self) -> &StreamRecord {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::execution::types::FieldValue;
    use std::collections::HashMap;

    #[test]
    fn test_shared_record_creation() {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        let record = StreamRecord::new(fields);

        let shared = SharedRecord::new(record);
        assert_eq!(shared.ref_count(), 1);
    }

    #[test]
    fn test_shared_record_clone() {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        let record = StreamRecord::new(fields);

        let shared1 = SharedRecord::new(record);
        let shared2 = shared1.clone();

        assert_eq!(shared1.ref_count(), 2);
        assert_eq!(shared2.ref_count(), 2);

        // Both point to the same data
        assert_eq!(
            shared1.as_ref().fields.get("id"),
            shared2.as_ref().fields.get("id")
        );
    }

    #[test]
    fn test_shared_record_try_unwrap() {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(42));
        let record = StreamRecord::new(fields);

        let shared = SharedRecord::new(record);

        // Should succeed - only one reference
        let unwrapped = shared.try_unwrap();
        assert!(unwrapped.is_some());

        let record = unwrapped.unwrap();
        assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(42)));
    }

    #[test]
    fn test_shared_record_try_unwrap_fails_with_multiple_refs() {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        let record = StreamRecord::new(fields);

        let shared1 = SharedRecord::new(record);
        let shared2 = shared1.clone();

        // Should fail - multiple references exist
        let unwrapped = shared1.try_unwrap();
        assert!(unwrapped.is_none());

        // shared2 still valid
        assert_eq!(shared2.ref_count(), 1);
    }
}
