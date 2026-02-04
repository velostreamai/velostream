//! Join Key Extractor
//!
//! Extracts join keys from stream records based on column specifications.
//! Supports multi-column keys with consistent string serialization for lookup.

use crate::velostream::sql::execution::types::system_columns;
use crate::velostream::sql::execution::{FieldValue, StreamRecord};

/// Extracts join keys from stream records
///
/// The extractor is configured with the column names to use for key extraction.
/// It supports both single-column and multi-column (composite) keys.
#[derive(Debug, Clone)]
pub struct JoinKeyExtractor {
    /// Column names to extract for the key (in order)
    columns: Vec<String>,
}

impl JoinKeyExtractor {
    /// Create a new key extractor for the given columns
    ///
    /// # Arguments
    /// * `columns` - Column names to use for key extraction
    ///
    /// # Example
    /// ```
    /// use velostream::velostream::sql::execution::join::JoinKeyExtractor;
    ///
    /// // Single-column key
    /// let extractor = JoinKeyExtractor::new(vec!["order_id".to_string()]);
    ///
    /// // Composite key
    /// let extractor = JoinKeyExtractor::new(vec![
    ///     "customer_id".to_string(),
    ///     "region".to_string(),
    /// ]);
    /// ```
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }

    /// Create a key extractor from a slice of column names
    pub fn from_slice(columns: &[&str]) -> Self {
        Self {
            columns: columns.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Extract the join key from a record as a composite string
    ///
    /// Returns `None` if any key column is missing from the record.
    /// Multi-column keys are separated by the NULL character (\0) to ensure
    /// unique composition (no collisions between different key combinations).
    ///
    /// # Arguments
    /// * `record` - The record to extract the key from
    ///
    /// # Returns
    /// * `Some(key)` - The extracted key as a string
    /// * `None` - If any key column is missing
    pub fn extract(&self, record: &StreamRecord) -> Option<String> {
        if self.columns.is_empty() {
            return Some(String::new());
        }

        let mut key_parts = Vec::with_capacity(self.columns.len());

        for col in &self.columns {
            // Strip table qualifier if present
            let bare_col = if col.contains('.') {
                col.split('.').next_back().unwrap_or(col)
            } else {
                col.as_str()
            };
            // System columns are always present on the record
            if system_columns::normalize_if_system_column(bare_col).is_some() {
                key_parts.push(Self::value_to_key_string(&record.resolve_column(col)));
            } else {
                match record
                    .fields
                    .get(col)
                    .or_else(|| record.fields.get(bare_col))
                {
                    Some(value) => key_parts.push(Self::value_to_key_string(value)),
                    None => return None, // Missing key column
                }
            }
        }

        // Join with NULL character for unique composite keys
        Some(key_parts.join("\0"))
    }

    /// Extract key with NULL handling - treats NULL fields as empty strings
    ///
    /// Unlike `extract()`, this method doesn't fail on NULL values but
    /// treats them as empty strings. Useful for outer joins.
    pub fn extract_with_nulls(&self, record: &StreamRecord) -> String {
        if self.columns.is_empty() {
            return String::new();
        }

        let key_parts: Vec<String> = self
            .columns
            .iter()
            .map(|col| {
                record
                    .fields
                    .get(col)
                    .map(Self::value_to_key_string)
                    .unwrap_or_default()
            })
            .collect();

        key_parts.join("\0")
    }

    /// Convert a FieldValue to a string for use in the join key
    ///
    /// This provides consistent serialization across all value types.
    fn value_to_key_string(value: &FieldValue) -> String {
        match value {
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => {
                // Use bit representation for consistent float handling
                format!("f:{}", f.to_bits())
            }
            FieldValue::String(s) => s.clone(),
            FieldValue::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
            FieldValue::Null => String::new(),
            FieldValue::Date(d) => d.to_string(),
            FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis().to_string(),
            FieldValue::Decimal(d) => d.to_string(),
            FieldValue::ScaledInteger(value, scale) => {
                // Normalize to canonical representation
                format!("si:{}:{}", value, scale)
            }
            FieldValue::Array(arr) => {
                // Hash arrays by their elements
                let parts: Vec<String> = arr.iter().map(Self::value_to_key_string).collect();
                format!("[{}]", parts.join(","))
            }
            FieldValue::Map(map) => {
                // Sort keys for deterministic representation
                let mut sorted: Vec<_> = map.iter().collect();
                sorted.sort_by(|a, b| a.0.cmp(b.0));
                let parts: Vec<String> = sorted
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, Self::value_to_key_string(v)))
                    .collect();
                format!("{{{}}}", parts.join(","))
            }
            FieldValue::Struct(fields) => {
                // Treat same as Map
                let mut sorted: Vec<_> = fields.iter().collect();
                sorted.sort_by(|a, b| a.0.cmp(b.0));
                let parts: Vec<String> = sorted
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, Self::value_to_key_string(v)))
                    .collect();
                format!("{{{}}}", parts.join(","))
            }
            FieldValue::Interval { value, unit } => {
                format!("interval:{}:{:?}", value, unit)
            }
        }
    }

    /// Get the column names used for key extraction
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Check if this is a single-column key
    pub fn is_single_column(&self) -> bool {
        self.columns.len() == 1
    }

    /// Check if this is a composite (multi-column) key
    pub fn is_composite(&self) -> bool {
        self.columns.len() > 1
    }
}

/// Builder for creating paired key extractors for left and right sides of a join
#[derive(Debug, Clone)]
pub struct JoinKeyExtractorPair {
    /// Extractor for left side records
    pub left: JoinKeyExtractor,
    /// Extractor for right side records
    pub right: JoinKeyExtractor,
}

impl JoinKeyExtractorPair {
    /// Create a key extractor pair from column pairs
    ///
    /// # Arguments
    /// * `pairs` - Pairs of (left_column, right_column) for the join condition
    ///
    /// # Example
    /// ```
    /// use velostream::velostream::sql::execution::join::JoinKeyExtractorPair;
    ///
    /// // Simple join: orders.order_id = shipments.order_id
    /// let extractors = JoinKeyExtractorPair::from_pairs(vec![
    ///     ("order_id".to_string(), "order_id".to_string()),
    /// ]);
    ///
    /// // Composite join: left.a = right.x AND left.b = right.y
    /// let extractors = JoinKeyExtractorPair::from_pairs(vec![
    ///     ("a".to_string(), "x".to_string()),
    ///     ("b".to_string(), "y".to_string()),
    /// ]);
    /// ```
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Self {
        let (left_cols, right_cols): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();

        Self {
            left: JoinKeyExtractor::new(left_cols),
            right: JoinKeyExtractor::new(right_cols),
        }
    }

    /// Create a pair where both sides use the same column names
    pub fn symmetric(columns: Vec<String>) -> Self {
        Self {
            left: JoinKeyExtractor::new(columns.clone()),
            right: JoinKeyExtractor::new(columns),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_record(fields: Vec<(&str, FieldValue)>) -> StreamRecord {
        let field_map: HashMap<String, FieldValue> = fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        StreamRecord::new(field_map)
    }

    #[test]
    fn test_single_column_extraction() {
        let extractor = JoinKeyExtractor::new(vec!["id".to_string()]);

        let record = make_record(vec![
            ("id", FieldValue::Integer(123)),
            ("name", FieldValue::String("test".to_string())),
        ]);

        let key = extractor.extract(&record);
        assert_eq!(key, Some("123".to_string()));
    }

    #[test]
    fn test_composite_key_extraction() {
        let extractor =
            JoinKeyExtractor::new(vec!["region".to_string(), "customer_id".to_string()]);

        let record = make_record(vec![
            ("region", FieldValue::String("US".to_string())),
            ("customer_id", FieldValue::Integer(42)),
            ("amount", FieldValue::Float(100.50)),
        ]);

        let key = extractor.extract(&record);
        assert!(key.is_some());
        let key_str = key.unwrap();
        // Composite keys are joined with NULL character
        assert!(key_str.contains('\0'));
        assert!(key_str.contains("US"));
        assert!(key_str.contains("42"));
    }

    #[test]
    fn test_missing_column_returns_none() {
        let extractor = JoinKeyExtractor::new(vec!["missing_col".to_string()]);

        let record = make_record(vec![("id", FieldValue::Integer(123))]);

        let key = extractor.extract(&record);
        assert!(key.is_none());
    }

    #[test]
    fn test_extract_with_nulls_handles_missing() {
        let extractor = JoinKeyExtractor::new(vec!["missing".to_string()]);

        let record = make_record(vec![("id", FieldValue::Integer(123))]);

        let key = extractor.extract_with_nulls(&record);
        assert_eq!(key, ""); // Missing treated as empty
    }

    #[test]
    fn test_null_value_in_key() {
        let extractor = JoinKeyExtractor::new(vec!["nullable".to_string()]);

        let record = make_record(vec![("nullable", FieldValue::Null)]);

        let key = extractor.extract(&record);
        assert_eq!(key, Some("".to_string()));
    }

    #[test]
    fn test_different_value_types() {
        let extractor = JoinKeyExtractor::new(vec!["field".to_string()]);

        // Integer
        let record = make_record(vec![("field", FieldValue::Integer(42))]);
        assert_eq!(extractor.extract(&record), Some("42".to_string()));

        // String
        let record = make_record(vec![("field", FieldValue::String("abc".to_string()))]);
        assert_eq!(extractor.extract(&record), Some("abc".to_string()));

        // Boolean
        let record = make_record(vec![("field", FieldValue::Boolean(true))]);
        assert_eq!(extractor.extract(&record), Some("true".to_string()));

        let record = make_record(vec![("field", FieldValue::Boolean(false))]);
        assert_eq!(extractor.extract(&record), Some("false".to_string()));
    }

    #[test]
    fn test_extractor_pair_from_pairs() {
        let pair = JoinKeyExtractorPair::from_pairs(vec![
            ("order_id".to_string(), "shipment_order_id".to_string()),
            ("region".to_string(), "ship_region".to_string()),
        ]);

        assert_eq!(pair.left.columns(), &["order_id", "region"]);
        assert_eq!(pair.right.columns(), &["shipment_order_id", "ship_region"]);
    }

    #[test]
    fn test_symmetric_extractor_pair() {
        let pair = JoinKeyExtractorPair::symmetric(vec!["id".to_string(), "type".to_string()]);

        assert_eq!(pair.left.columns(), pair.right.columns());
        assert_eq!(pair.left.columns(), &["id", "type"]);
    }

    #[test]
    fn test_empty_columns() {
        let extractor = JoinKeyExtractor::new(vec![]);

        let record = make_record(vec![("id", FieldValue::Integer(123))]);

        // Empty column list produces empty key
        let key = extractor.extract(&record);
        assert_eq!(key, Some(String::new()));
    }

    #[test]
    fn test_float_consistency() {
        let extractor = JoinKeyExtractor::new(vec!["price".to_string()]);

        let record1 = make_record(vec![("price", FieldValue::Float(123.456))]);
        let record2 = make_record(vec![("price", FieldValue::Float(123.456))]);

        // Same float values should produce identical keys
        assert_eq!(extractor.extract(&record1), extractor.extract(&record2));
    }

    #[test]
    fn test_composite_key_uniqueness() {
        let extractor = JoinKeyExtractor::new(vec!["a".to_string(), "b".to_string()]);

        // These should produce different keys even though concatenated values look similar
        let record1 = make_record(vec![
            ("a", FieldValue::String("12".to_string())),
            ("b", FieldValue::String("34".to_string())),
        ]);

        let record2 = make_record(vec![
            ("a", FieldValue::String("123".to_string())),
            ("b", FieldValue::String("4".to_string())),
        ]);

        let key1 = extractor.extract(&record1).unwrap();
        let key2 = extractor.extract(&record2).unwrap();

        // NULL separator ensures uniqueness
        assert_ne!(key1, key2);
    }
}
