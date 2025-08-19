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
    fn field_value_to_group_key(value: &FieldValue) -> String {
        match value {
            FieldValue::String(s) => s.clone(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Null => "NULL".to_string(),
            FieldValue::Timestamp(ts) => ts.to_string(),
            FieldValue::Array(arr) => {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(Self::field_value_to_group_key)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            FieldValue::Map(obj) | FieldValue::Struct(obj) => {
                let mut pairs: Vec<_> = obj
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, Self::field_value_to_group_key(v)))
                    .collect();
                pairs.sort(); // Ensure consistent ordering
                format!("{{{}}}", pairs.join(","))
            }
            FieldValue::Date(date) => date.to_string(),
            FieldValue::Decimal(decimal) => decimal.to_string(),
            FieldValue::Interval { value, unit } => format!("{}_{:?}", value, unit),
        }
    }

    /// Check if a record matches the GROUP BY key
    pub fn record_matches_group_key(
        expressions: &[Expr],
        record: &StreamRecord,
        target_key: &[String],
    ) -> Result<bool, SqlError> {
        let record_key = Self::generate_group_key(expressions, record)?;
        Ok(record_key == target_key)
    }

    /// Get all records that belong to a specific group from a buffer
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::sql::ast::Expr;
    use std::collections::HashMap;

    fn create_test_record(fields: Vec<(&str, FieldValue)>) -> StreamRecord {
        let mut field_map = HashMap::new();
        for (key, value) in fields {
            field_map.insert(key.to_string(), value);
        }

        StreamRecord {
            fields: field_map,
            timestamp: 0,
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn test_field_value_to_group_key() {
        assert_eq!(
            GroupByStateManager::field_value_to_group_key(&FieldValue::String("test".to_string())),
            "test"
        );
        assert_eq!(
            GroupByStateManager::field_value_to_group_key(&FieldValue::Integer(42)),
            "42"
        );
        assert_eq!(
            GroupByStateManager::field_value_to_group_key(&FieldValue::Float(std::f64::consts::PI)),
            &std::f64::consts::PI.to_string()
        );
        assert_eq!(
            GroupByStateManager::field_value_to_group_key(&FieldValue::Boolean(true)),
            "true"
        );
        assert_eq!(
            GroupByStateManager::field_value_to_group_key(&FieldValue::Null),
            "NULL"
        );
    }

    #[test]
    fn test_generate_group_key_simple() {
        let record = create_test_record(vec![
            ("category", FieldValue::String("electronics".to_string())),
            ("priority", FieldValue::Integer(1)),
        ]);

        let expressions = vec![
            Expr::Column("category".to_string()),
            Expr::Column("priority".to_string()),
        ];

        let result = GroupByStateManager::generate_group_key(&expressions, &record);
        assert!(result.is_ok());
        let key = result.unwrap();
        assert_eq!(key, vec!["electronics".to_string(), "1".to_string()]);
    }

    #[test]
    fn test_record_matches_group_key() {
        let record = create_test_record(vec![(
            "category",
            FieldValue::String("electronics".to_string()),
        )]);

        let expressions = vec![Expr::Column("category".to_string())];
        let target_key = vec!["electronics".to_string()];

        let result =
            GroupByStateManager::record_matches_group_key(&expressions, &record, &target_key);
        assert!(result.is_ok());
        assert!(result.unwrap());

        let wrong_key = vec!["books".to_string()];
        let result2 =
            GroupByStateManager::record_matches_group_key(&expressions, &record, &wrong_key);
        assert!(result2.is_ok());
        assert!(!result2.unwrap());
    }

    #[test]
    fn test_extract_group_keys() {
        let records = vec![
            create_test_record(vec![(
                "category",
                FieldValue::String("electronics".to_string()),
            )]),
            create_test_record(vec![("category", FieldValue::String("books".to_string()))]),
            create_test_record(vec![(
                "category",
                FieldValue::String("electronics".to_string()),
            )]),
        ];

        let expressions = vec![Expr::Column("category".to_string())];

        let result = GroupByStateManager::extract_group_keys(&expressions, &records);
        assert!(result.is_ok());
        let keys = result.unwrap();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&vec!["electronics".to_string()]));
        assert!(keys.contains(&vec!["books".to_string()]));
    }
}
