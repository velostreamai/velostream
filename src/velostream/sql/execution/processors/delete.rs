/*!
# DELETE Processor

Handles DELETE operations for removing records in streaming contexts.

## Supported Operations

1. **Conditional deletes**: `DELETE FROM table WHERE condition`
2. **Unconditional deletes**: `DELETE FROM table` (deletes all matching records)
3. **Complex WHERE clauses**: `DELETE FROM table WHERE col IN (SELECT ...)`

## Streaming Semantics

DELETE operations in streaming contexts have special semantics:
- Creates tombstone records (null payload) to indicate deletion
- Original records may remain for audit trail depending on configuration
- WHERE clause determines which records to delete
- Provides delete confirmation events
- Maintains event order and causality

## Examples

```sql
-- Simple conditional delete
DELETE FROM orders WHERE status = 'CANCELLED';

-- Delete with complex condition
DELETE FROM users WHERE last_login < NOW() - INTERVAL '365' DAYS;

-- Delete with subquery
DELETE FROM temp_data WHERE id IN (SELECT id FROM processed_records);
```

## Tombstone Records

In streaming/Kafka contexts, DELETE creates tombstone records:
- Key: Same as original record
- Value: null (tombstone marker)
- Headers: Metadata about the delete operation
- Timestamp: When the delete occurred
*/

use crate::velostream::sql::ast::Expr;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::expression::evaluator::ExpressionEvaluator;
use crate::velostream::sql::execution::{FieldValue, StreamRecord};
use std::collections::HashMap;

/// Processor for DELETE operations
pub struct DeleteProcessor;

impl DeleteProcessor {
    /// Process a DELETE statement
    pub fn process_delete(
        table_name: &str,
        where_clause: &Option<Expr>,
        input_record: &StreamRecord,
    ) -> Result<Option<StreamRecord>, SqlError> {
        log::info!("Processing DELETE FROM {} statement", table_name);

        // Evaluate WHERE clause to see if this record should be deleted
        if let Some(where_expr) = where_clause {
            if !ExpressionEvaluator::evaluate_expression(where_expr, input_record)? {
                log::debug!("Record does not match WHERE clause, skipping delete");
                return Ok(None); // Record doesn't match WHERE condition
            }
        }

        log::info!("Record matches WHERE clause, creating delete tombstone");

        // Create tombstone record for deletion
        let tombstone_record = Self::create_tombstone_record(input_record, table_name)?;

        Ok(Some(tombstone_record))
    }

    /// Create a tombstone record to represent the deletion
    pub fn create_tombstone_record(
        original_record: &StreamRecord,
        table_name: &str,
    ) -> Result<StreamRecord, SqlError> {
        // In streaming contexts, a tombstone record typically has:
        // - Same key as original record (for proper partitioning)
        // - Empty/null fields (to indicate deletion)
        // - Special headers to mark it as a tombstone
        // - New timestamp for when the delete occurred

        let mut tombstone_fields = HashMap::new();

        // Preserve key fields if they exist (for proper partitioning)
        // In a full implementation, this would be configurable based on table schema
        if let Some(id_field) = original_record.fields.get("id") {
            tombstone_fields.insert("id".to_string(), id_field.clone());
        }
        if let Some(key_field) = original_record.fields.get("key") {
            tombstone_fields.insert("key".to_string(), key_field.clone());
        }

        // Add tombstone marker fields
        tombstone_fields.insert("__deleted".to_string(), FieldValue::Boolean(true));
        tombstone_fields.insert(
            "__deleted_at".to_string(),
            FieldValue::String(chrono::Utc::now().to_rfc3339()),
        );

        let tombstone_record = StreamRecord {
            fields: tombstone_fields,
            timestamp: chrono::Utc::now().timestamp_millis(), // New timestamp for delete event
            offset: original_record.offset + 1, // Increment offset to show this is a new event
            partition: original_record.partition,
            headers: {
                let mut headers = original_record.headers.clone();
                // Add metadata to indicate this is a delete operation
                headers.insert("operation".to_string(), "DELETE".to_string());
                headers.insert("table_name".to_string(), table_name.to_string());
                headers.insert("deleted_at".to_string(), chrono::Utc::now().to_rfc3339());
                headers.insert(
                    "original_timestamp".to_string(),
                    original_record.timestamp.to_string(),
                );
                headers.insert(
                    "original_offset".to_string(),
                    original_record.offset.to_string(),
                );
                headers.insert("tombstone".to_string(), "true".to_string());
                headers
            },
            event_time: None,
            topic: None,
            key: None,
        };

        log::info!("DELETE: Created tombstone record for table {}", table_name);
        Ok(tombstone_record)
    }

    /// Validate DELETE operation before processing
    pub fn validate_delete(table_name: &str, where_clause: &Option<Expr>) -> Result<(), SqlError> {
        log::debug!("Validating DELETE FROM {} statement", table_name);

        // Basic validation
        if table_name.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Table name cannot be empty".to_string(),
                query: Some("DELETE FROM".to_string()),
            });
        }

        // Validate WHERE clause if present
        if let Some(_where_expr) = where_clause {
            // TODO: Add WHERE clause validation:
            // - Referenced columns exist in table
            // - Expression is valid and returns boolean
            // - Subqueries are properly formed
        }

        // TODO: Add more validation:
        // - Table exists and is accessible
        // - User has DELETE permissions on table
        // - Check for dangerous operations (DELETE without WHERE)

        Ok(())
    }

    /// Check if record matches DELETE WHERE clause
    pub fn matches_where_clause(
        where_clause: &Option<Expr>,
        record: &StreamRecord,
    ) -> Result<bool, SqlError> {
        match where_clause {
            Some(expr) => ExpressionEvaluator::evaluate_expression(expr, record),
            None => Ok(true), // No WHERE clause means all records match
        }
    }

    /// Get columns referenced in DELETE WHERE clause
    pub fn get_referenced_columns(where_clause: &Option<Expr>) -> Vec<String> {
        match where_clause {
            Some(expr) => expr.get_columns(),
            None => Vec::new(),
        }
    }

    /// Create a soft delete record (alternative to tombstone)
    pub fn create_soft_delete_record(
        original_record: &StreamRecord,
        table_name: &str,
    ) -> Result<StreamRecord, SqlError> {
        // Soft delete keeps the record but marks it as deleted
        let mut soft_delete_fields = original_record.fields.clone();

        // Add soft delete markers
        soft_delete_fields.insert("deleted".to_string(), FieldValue::Boolean(true));
        soft_delete_fields.insert(
            "deleted_at".to_string(),
            FieldValue::String(chrono::Utc::now().to_rfc3339()),
        );

        let soft_delete_record = StreamRecord {
            fields: soft_delete_fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: original_record.offset + 1,
            partition: original_record.partition,
            headers: {
                let mut headers = original_record.headers.clone();
                headers.insert("operation".to_string(), "SOFT_DELETE".to_string());
                headers.insert("table_name".to_string(), table_name.to_string());
                headers.insert("deleted_at".to_string(), chrono::Utc::now().to_rfc3339());
                headers
            },
            event_time: None,
            topic: None,
            key: None,
        };

        log::info!(
            "DELETE: Created soft delete record for table {}",
            table_name
        );
        Ok(soft_delete_record)
    }

    /// Check if a record is a tombstone (deleted record)
    pub fn is_tombstone_record(record: &StreamRecord) -> bool {
        // Check for tombstone markers in headers
        if let Some(tombstone_marker) = record.headers.get("tombstone") {
            return tombstone_marker == "true";
        }

        // Check for tombstone marker in fields
        if let Some(FieldValue::Boolean(true)) = record.fields.get("__deleted") {
            return true;
        }

        // Check if record has minimal fields (typical tombstone pattern)
        record.fields.len() <= 2 && record.fields.contains_key("id")
    }

    /// Extract original record information from tombstone
    pub fn extract_tombstone_metadata(tombstone: &StreamRecord) -> HashMap<String, String> {
        let mut metadata = HashMap::new();

        // Extract metadata from headers
        for (key, value) in &tombstone.headers {
            if key.starts_with("original_") || key == "table_name" || key == "deleted_at" {
                metadata.insert(key.clone(), value.clone());
            }
        }

        // Extract key fields that were preserved
        for (key, value) in &tombstone.fields {
            if key == "id" || key == "key" {
                metadata.insert(format!("key_{}", key), value.to_display_string());
            }
        }

        metadata
    }
}
