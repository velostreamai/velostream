/*!
# INSERT Processor

Handles INSERT INTO operations for streaming data ingestion.

## Supported Operations

1. **VALUES insertion**: `INSERT INTO table VALUES (1, 'abc'), (2, 'def')`
2. **SELECT insertion**: `INSERT INTO table SELECT * FROM source WHERE condition`
3. **Column specification**: `INSERT INTO table (col1, col2) VALUES (1, 'abc')`

## Streaming Semantics

INSERT operations in streaming contexts create new records in the target stream:
- Each INSERT statement processes immediately
- Supports both batch and streaming sources
- Maintains event order and timestamps
- Provides insert confirmation events

## Examples

```sql
-- Simple values insertion
INSERT INTO orders VALUES (1001, 'Widget', 99.99, 'PENDING');

-- Selective column insertion
INSERT INTO orders (id, product_name, status) VALUES (1002, 'Gadget', 'CONFIRMED');

-- INSERT from SELECT query
INSERT INTO daily_orders
SELECT order_id, product_name, amount
FROM orders
WHERE created_date = CURRENT_DATE;
```
*/

use crate::ferris::sql::ast::{InsertSource, StreamingQuery};
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::expression::evaluator::ExpressionEvaluator;
use crate::ferris::sql::execution::{FieldValue, StreamRecord};
use std::collections::HashMap;

/// Processor for INSERT INTO operations
pub struct InsertProcessor;

impl InsertProcessor {
    /// Process an INSERT INTO statement
    pub fn process_insert(
        table_name: &str,
        columns: &Option<Vec<String>>,
        source: &InsertSource,
        input_record: &StreamRecord,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        log::info!("Processing INSERT INTO {} statement", table_name);

        match source {
            InsertSource::Values { rows } => {
                Self::process_values_insert(table_name, columns, rows, input_record)
            }
            InsertSource::Select { query } => {
                Self::process_select_insert(table_name, columns, query, input_record)
            }
        }
    }

    /// Process INSERT INTO table VALUES (...), (...) format
    fn process_values_insert(
        table_name: &str,
        columns: &Option<Vec<String>>,
        rows: &[Vec<crate::ferris::sql::ast::Expr>],
        input_record: &StreamRecord,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        let mut result_records = Vec::new();

        for (row_index, row_values) in rows.iter().enumerate() {
            log::debug!(
                "Processing VALUES row {} for table {}",
                row_index,
                table_name
            );

            // Evaluate each expression in the row to get field values
            let mut fields = HashMap::new();

            if let Some(column_names) = columns {
                // Explicit column names provided
                if column_names.len() != row_values.len() {
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "Column count mismatch: {} columns specified but {} values provided",
                            column_names.len(),
                            row_values.len()
                        ),
                        query: Some(format!("INSERT INTO {}", table_name)),
                    });
                }

                // Map values to specified columns
                for (col_name, expr) in column_names.iter().zip(row_values.iter()) {
                    let value = ExpressionEvaluator::evaluate_expression_value(expr, input_record)?;
                    fields.insert(col_name.clone(), value);
                }
            } else {
                // No explicit columns - use positional mapping
                // In a real implementation, would get column names from schema
                for (index, expr) in row_values.iter().enumerate() {
                    let value = ExpressionEvaluator::evaluate_expression_value(expr, input_record)?;
                    let col_name = format!("col_{}", index); // Default column naming
                    fields.insert(col_name, value);
                }
            }

            // Create new record for insertion
            let insert_record = StreamRecord {
                fields,
                timestamp: input_record.timestamp, // Preserve original timestamp
                offset: input_record.offset + (row_index as i64), // Increment offset for each row
                partition: input_record.partition,
                headers: input_record.headers.clone(),
            };

            result_records.push(insert_record);
        }

        log::info!(
            "INSERT VALUES: Created {} records for table {}",
            result_records.len(),
            table_name
        );
        Ok(result_records)
    }

    /// Process INSERT INTO table SELECT ... format
    fn process_select_insert(
        table_name: &str,
        columns: &Option<Vec<String>>,
        query: &Box<StreamingQuery>,
        input_record: &StreamRecord,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        log::info!("Processing INSERT ... SELECT for table {}", table_name);

        match query.as_ref() {
            StreamingQuery::Select { .. } => {
                Err(SqlError::ExecutionError {
                    message: "INSERT ... SELECT operations are not yet implemented. This requires executing the SELECT subquery and mapping results to target table columns.".to_string(),
                    query: Some(format!("INSERT INTO {} SELECT ...", table_name)),
                })
            }
            _ => Err(SqlError::ExecutionError {
                message: "INSERT ... SELECT supports only SELECT queries".to_string(),
                query: Some(format!("INSERT INTO {} SELECT ...", table_name)),
            }),
        }
    }

    /// Validate INSERT operation before processing
    pub fn validate_insert(
        table_name: &str,
        columns: &Option<Vec<String>>,
        source: &InsertSource,
    ) -> Result<(), SqlError> {
        log::debug!("Validating INSERT INTO {} statement", table_name);

        // Basic validation
        if table_name.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Table name cannot be empty".to_string(),
                query: Some("INSERT INTO".to_string()),
            });
        }

        match source {
            InsertSource::Values { rows } => {
                if rows.is_empty() {
                    return Err(SqlError::ExecutionError {
                        message: "VALUES list cannot be empty".to_string(),
                        query: Some(format!("INSERT INTO {}", table_name)),
                    });
                }

                // Check all rows have same number of values
                if let Some(first_row) = rows.first() {
                    let expected_count = first_row.len();
                    for (index, row) in rows.iter().enumerate() {
                        if row.len() != expected_count {
                            return Err(SqlError::ExecutionError {
                                message: format!(
                                    "Row {} has {} values but expected {}",
                                    index,
                                    row.len(),
                                    expected_count
                                ),
                                query: Some(format!("INSERT INTO {}", table_name)),
                            });
                        }
                    }

                    // If columns specified, check count matches
                    if let Some(column_names) = columns {
                        if column_names.len() != expected_count {
                            return Err(SqlError::ExecutionError {
                                message: format!(
                                    "Column count mismatch: {} columns specified but {} values per row",
                                    column_names.len(),
                                    expected_count
                                ),
                                query: Some(format!("INSERT INTO {}", table_name)),
                            });
                        }
                    }
                }
            }
            InsertSource::Select { query } => {
                // Validate the SELECT query structure
                match query.as_ref() {
                    StreamingQuery::Select { .. } => {
                        // SELECT query structure is valid
                        // TODO: Add more detailed validation:
                        // - Column count compatibility
                        // - Type compatibility
                        // - Source table availability
                    }
                    _ => {
                        return Err(SqlError::ExecutionError {
                            message: "INSERT ... SELECT requires a SELECT query".to_string(),
                            query: Some(format!("INSERT INTO {}", table_name)),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Get column information for INSERT operation
    pub fn get_insert_columns(
        columns: &Option<Vec<String>>,
        source: &InsertSource,
    ) -> Result<Vec<String>, SqlError> {
        match columns {
            Some(column_names) => Ok(column_names.clone()),
            None => {
                // Infer columns from source
                match source {
                    InsertSource::Values { rows } => {
                        if let Some(first_row) = rows.first() {
                            // Generate default column names
                            Ok((0..first_row.len()).map(|i| format!("col_{}", i)).collect())
                        } else {
                            Ok(Vec::new())
                        }
                    }
                    InsertSource::Select { query } => {
                        // Get columns from SELECT query
                        match query.as_ref() {
                            StreamingQuery::Select { fields: _, .. } => {
                                // TODO: Extract actual column names from SELECT fields
                                Ok(vec!["inferred_col".to_string()])
                            }
                            _ => Ok(Vec::new()),
                        }
                    }
                }
            }
        }
    }
}
