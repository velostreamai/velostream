//! GROUP BY state management for streaming SQL aggregations.
//!
//! This module provides utilities for managing GROUP BY state across streaming
//! records, including group key generation and state lifecycle management.

use super::super::types::{FieldValue, StreamRecord};
use crate::ferris::sql::ast::Expr;
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::expression::ExpressionEvaluator;

/// Utilities for GROUP BY state management
pub struct GroupByStateManager;

impl GroupByStateManager {
    /// Generate group key values for a record based on GROUP BY expressions
    #[doc(hidden)]
    pub fn generate_group_key(
        expressions: &[Expr],
        record: &StreamRecord,
    ) -> Result<Vec<String>, SqlError> {
        let mut key_values = Vec::new();

        for expr in expressions {
            let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
            key_values.push(Self::field_value_to_group_key(&value));
        }

        Ok(key_values)
    }

    /// Convert a FieldValue to a string representation for group key
    /// Uses comprehensive formatting matching engine implementation for consistency
    #[doc(hidden)]
    pub fn field_value_to_group_key(value: &FieldValue) -> String {
        match value {
            FieldValue::String(s) => s.clone(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Null => "NULL".to_string(),
            FieldValue::Date(d) => d.format("%Y-%m-%d").to_string(),
            FieldValue::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            FieldValue::Decimal(dec) => dec.to_string(),
            FieldValue::Array(arr) => {
                // For arrays, create a string representation
                let elements: Vec<String> = arr
                    .iter()
                    .map(Self::field_value_to_group_key)
                    .collect();
                format!("[{}]", elements.join(","))
            }
            FieldValue::Map(map) => {
                // For maps, create a sorted string representation
                let mut entries: Vec<_> = map.iter().collect();
                entries.sort_by_key(|(k, _)| k.as_str());
                let map_str: Vec<String> = entries
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, Self::field_value_to_group_key(v)))
                    .collect();
                format!("{{{}}}", map_str.join(","))
            }
            FieldValue::Struct(fields) => {
                // For structs, create a sorted string representation with parentheses
                let mut entries: Vec<_> = fields.iter().collect();
                entries.sort_by_key(|(k, _)| k.as_str());
                let struct_str: Vec<String> = entries
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, Self::field_value_to_group_key(v)))
                    .collect();
                format!("({})", struct_str.join(","))
            }
            FieldValue::Interval { value, unit } => {
                format!("INTERVAL:{}:{:?}", value, unit)
            }
        }
    }

    /// Check if a record matches the GROUP BY key
    #[doc(hidden)]
    pub fn record_matches_group_key(
        expressions: &[Expr],
        record: &StreamRecord,
        target_key: &[String],
    ) -> Result<bool, SqlError> {
        let record_key = Self::generate_group_key(expressions, record)?;
        Ok(record_key == target_key)
    }

    /// Get all records that belong to a specific group from a buffer
    #[doc(hidden)]
    pub fn get_group_records<'a>(
        expressions: &[Expr],
        records: &'a [StreamRecord],
        target_key: &[String],
    ) -> Result<Vec<&'a StreamRecord>, SqlError> {
        let mut group_records = Vec::new();

        for record in records {
            if Self::record_matches_group_key(expressions, record, target_key)? {
                group_records.push(record);
            }
        }

        Ok(group_records)
    }

    /// Extract all unique group keys from a collection of records
    #[doc(hidden)]
    pub fn extract_group_keys(
        expressions: &[Expr],
        records: &[StreamRecord],
    ) -> Result<Vec<Vec<String>>, SqlError> {
        let mut keys = Vec::new();

        for record in records {
            let key = Self::generate_group_key(expressions, record)?;
            if !keys.contains(&key) {
                keys.push(key);
            }
        }

        Ok(keys)
    }
}
