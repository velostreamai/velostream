/*!
# UPDATE Processor

Handles UPDATE operations for modifying existing records in streaming contexts.

## Supported Operations

1. **Conditional updates**: `UPDATE table SET col1 = value WHERE condition`
2. **Expression-based updates**: `UPDATE table SET price = price * 1.1 WHERE category = 'electronics'`
3. **Multiple column updates**: `UPDATE table SET col1 = val1, col2 = val2 WHERE condition`

## Streaming Semantics

UPDATE operations in streaming contexts have special semantics:
- Creates new records with updated values (streaming is append-only)
- Original records remain in the stream (for audit trail)
- WHERE clause determines which records to update
- Supports expression evaluation for dynamic updates
- Maintains event order and provides update confirmation

## Examples

```sql
-- Simple field update
UPDATE orders SET status = 'SHIPPED' WHERE order_id = 1001;

-- Expression-based update
UPDATE products SET price = price * 0.9, updated_at = NOW() WHERE category = 'clearance';

-- Conditional update with complex WHERE
UPDATE users SET last_login = CURRENT_TIMESTAMP
WHERE user_id IN (SELECT user_id FROM active_sessions);
```
*/

use crate::ferris::sql::ast::Expr;
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::expression::evaluator::ExpressionEvaluator;
use crate::ferris::sql::execution::{FieldValue, StreamRecord};
use std::collections::HashMap;

/// Processor for UPDATE operations
pub struct UpdateProcessor;

impl UpdateProcessor {
    /// Process an UPDATE statement
    pub fn process_update(
        table_name: &str,
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
        input_record: &StreamRecord,
    ) -> Result<Option<StreamRecord>, SqlError> {
        log::info!("Processing UPDATE {} statement", table_name);

        // Evaluate WHERE clause first to see if this record should be updated
        if let Some(where_expr) = where_clause {
            if !ExpressionEvaluator::evaluate_expression(where_expr, input_record)? {
                log::debug!("Record does not match WHERE clause, skipping update");
                return Ok(None); // Record doesn't match WHERE condition
            }
        }

        log::debug!("Record matches WHERE clause, applying updates");

        // Apply assignments to create updated record
        let mut updated_fields = input_record.fields.clone();
        let mut changes_made = 0;

        for (column_name, value_expr) in assignments {
            log::debug!("Updating column '{}' with expression", column_name);

            // Evaluate the assignment expression
            let new_value =
                ExpressionEvaluator::evaluate_expression_value(value_expr, input_record)?;

            // Update the field (or add if it doesn't exist)
            updated_fields.insert(column_name.clone(), new_value);
            changes_made += 1;
        }

        if changes_made > 0 {
            log::info!("UPDATE: Applied {} column changes to record", changes_made);

            // Create updated record with new timestamp
            let updated_record = StreamRecord {
                fields: updated_fields,
                timestamp: chrono::Utc::now().timestamp_millis(), // New timestamp for update event
                offset: input_record.offset + 1, // Increment offset to show this is a new event
                partition: input_record.partition,
                headers: {
                    let mut headers = input_record.headers.clone();
                    // Add metadata to indicate this is an update operation
                    headers.insert("operation".to_string(), "UPDATE".to_string());
                    headers.insert("updated_at".to_string(), chrono::Utc::now().to_rfc3339());
                    headers
                },
            };

            Ok(Some(updated_record))
        } else {
            log::debug!("No changes made during UPDATE operation");
            Ok(None)
        }
    }

    /// Validate UPDATE operation before processing
    pub fn validate_update(
        table_name: &str,
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
    ) -> Result<(), SqlError> {
        log::debug!("Validating UPDATE {} statement", table_name);

        // Basic validation
        if table_name.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Table name cannot be empty".to_string(),
                query: Some("UPDATE".to_string()),
            });
        }

        if assignments.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "UPDATE must have at least one SET assignment".to_string(),
                query: Some(format!("UPDATE {}", table_name)),
            });
        }

        // Validate assignment expressions
        for (column_name, _expr) in assignments {
            if column_name.is_empty() {
                return Err(SqlError::ExecutionError {
                    message: "Column name in SET clause cannot be empty".to_string(),
                    query: Some(format!("UPDATE {}", table_name)),
                });
            }

            // TODO: Add more detailed validation:
            // - Column exists in table schema
            // - Type compatibility between expression and column
            // - Expression validity
        }

        // Validate WHERE clause if present
        if let Some(_where_expr) = where_clause {
            // TODO: Add WHERE clause validation:
            // - Referenced columns exist
            // - Expression is valid
            // - Returns boolean result
        }

        Ok(())
    }

    /// Get affected columns from UPDATE statement
    pub fn get_affected_columns(
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
    ) -> Vec<String> {
        let mut columns = Vec::new();

        // Add columns from SET assignments
        for (column_name, expr) in assignments {
            columns.push(column_name.clone());
            // Also add columns referenced in the assignment expression
            columns.extend(expr.get_columns());
        }

        // Add columns from WHERE clause
        if let Some(where_expr) = where_clause {
            columns.extend(where_expr.get_columns());
        }

        columns.sort();
        columns.dedup();
        columns
    }

    /// Check if record matches UPDATE WHERE clause
    pub fn matches_where_clause(
        where_clause: &Option<Expr>,
        record: &StreamRecord,
    ) -> Result<bool, SqlError> {
        match where_clause {
            Some(expr) => ExpressionEvaluator::evaluate_expression(expr, record),
            None => Ok(true), // No WHERE clause means all records match
        }
    }

    /// Apply assignments to create updated field values
    pub fn apply_assignments(
        assignments: &[(String, Expr)],
        record: &StreamRecord,
    ) -> Result<HashMap<String, FieldValue>, SqlError> {
        let mut updated_fields = record.fields.clone();

        for (column_name, value_expr) in assignments {
            let new_value = ExpressionEvaluator::evaluate_expression_value(value_expr, record)?;
            updated_fields.insert(column_name.clone(), new_value);
        }

        Ok(updated_fields)
    }

    /// Create update metadata for tracking changes
    pub fn create_update_metadata(
        original_record: &StreamRecord,
        assignments: &[(String, Expr)],
    ) -> HashMap<String, String> {
        let mut metadata = HashMap::new();

        metadata.insert("operation".to_string(), "UPDATE".to_string());
        metadata.insert("updated_at".to_string(), chrono::Utc::now().to_rfc3339());
        metadata.insert(
            "original_timestamp".to_string(),
            original_record.timestamp.to_string(),
        );
        metadata.insert(
            "original_offset".to_string(),
            original_record.offset.to_string(),
        );
        metadata.insert(
            "updated_columns".to_string(),
            assignments
                .iter()
                .map(|(col, _)| col.clone())
                .collect::<Vec<_>>()
                .join(","),
        );

        metadata
    }
}
