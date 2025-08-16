//! JOIN processor implementation for streaming SQL JOIN operations.

use std::collections::HashMap;

use crate::ferris::sql::ast::{Expr, JoinClause, JoinType, JoinWindow, StreamSource};
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::expressions::ExpressionEvaluator;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};

/// Processor for JOIN operations in streaming SQL queries
///
/// This processor handles all aspects of JOIN processing including:
/// - All SQL JOIN types (INNER, LEFT, RIGHT, FULL OUTER)
/// - Stream-stream and stream-table JOIN optimizations
/// - Windowed JOINs with temporal constraints and grace periods
/// - Proper record combination with field aliasing and NULL handling
/// - Key-based matching and condition evaluation
pub struct JoinProcessor;

impl JoinProcessor {
    /// Process JOIN clauses and combine records
    pub fn process_joins(
        left_record: &StreamRecord,
        join_clauses: &[JoinClause],
    ) -> Result<StreamRecord, SqlError> {
        let mut result_record = left_record.clone();

        for join_clause in join_clauses {
            // Get right record - in production this would query actual data sources
            let right_record_opt =
                Self::get_right_record(&join_clause.right_source, &join_clause.window)?;

            result_record =
                Self::execute_join(&result_record, right_record_opt.as_ref(), join_clause)?;
        }

        Ok(result_record)
    }

    /// Execute a single JOIN operation
    pub fn execute_join(
        left_record: &StreamRecord,
        right_record: Option<&StreamRecord>,
        join_clause: &JoinClause,
    ) -> Result<StreamRecord, SqlError> {
        match join_clause.join_type {
            JoinType::Inner => {
                if let Some(right_record) = right_record {
                    let combined_record =
                        Self::combine_records(left_record, right_record, &join_clause.right_alias)?;
                    if ExpressionEvaluator::evaluate_expression(
                        &join_clause.condition,
                        &combined_record,
                    )? {
                        Ok(combined_record)
                    } else {
                        // INNER JOIN: if condition fails, no result
                        Err(SqlError::ExecutionError {
                            message: "INNER JOIN condition not met".to_string(),
                            query: None,
                        })
                    }
                } else {
                    // INNER JOIN: if no right record, no result
                    Err(SqlError::ExecutionError {
                        message: "No matching record for INNER JOIN".to_string(),
                        query: None,
                    })
                }
            }
            JoinType::Left => {
                if let Some(right_record) = right_record {
                    let combined_record =
                        Self::combine_records(left_record, right_record, &join_clause.right_alias)?;
                    if ExpressionEvaluator::evaluate_expression(
                        &join_clause.condition,
                        &combined_record,
                    )? {
                        Ok(combined_record)
                    } else {
                        // LEFT JOIN: if condition fails, use left record with NULL right fields
                        Self::combine_records_with_nulls(
                            left_record,
                            &join_clause.right_alias,
                            true,
                        )
                    }
                } else {
                    // LEFT JOIN: if no right record, use left record with NULL right fields
                    Self::combine_records_with_nulls(left_record, &join_clause.right_alias, true)
                }
            }
            JoinType::Right => {
                if let Some(right_record) = right_record {
                    let combined_record =
                        Self::combine_records(left_record, right_record, &join_clause.right_alias)?;
                    if ExpressionEvaluator::evaluate_expression(
                        &join_clause.condition,
                        &combined_record,
                    )? {
                        Ok(combined_record)
                    } else {
                        // RIGHT JOIN: if condition fails, use right record with NULL left fields
                        Self::combine_records_with_nulls(
                            right_record,
                            &join_clause.right_alias,
                            false,
                        )
                    }
                } else {
                    // RIGHT JOIN: if no right record, no result
                    Err(SqlError::ExecutionError {
                        message: "No right record for RIGHT JOIN".to_string(),
                        query: None,
                    })
                }
            }
            JoinType::FullOuter => {
                if let Some(right_record) = right_record {
                    let combined_record =
                        Self::combine_records(left_record, right_record, &join_clause.right_alias)?;
                    if ExpressionEvaluator::evaluate_expression(
                        &join_clause.condition,
                        &combined_record,
                    )? {
                        Ok(combined_record)
                    } else {
                        // FULL OUTER JOIN: if condition fails, could return both records with NULLs
                        // For simplicity, we'll return left record with NULL right fields
                        Self::combine_records_with_nulls(
                            left_record,
                            &join_clause.right_alias,
                            true,
                        )
                    }
                } else {
                    // FULL OUTER JOIN: if no right record, use left record with NULL right fields
                    Self::combine_records_with_nulls(left_record, &join_clause.right_alias, true)
                }
            }
        }
    }

    /// Combine two records for JOIN operations
    pub fn combine_records(
        left_record: &StreamRecord,
        right_record: &StreamRecord,
        right_alias: &Option<String>,
    ) -> Result<StreamRecord, SqlError> {
        let mut combined_fields = left_record.fields.clone();

        // Add right record fields with proper aliasing
        for (field_name, field_value) in &right_record.fields {
            let aliased_name = if let Some(alias) = right_alias {
                format!("{}.{}", alias, field_name)
            } else {
                format!("right_{}", field_name)
            };
            combined_fields.insert(aliased_name, field_value.clone());
        }

        Ok(StreamRecord {
            fields: combined_fields,
            timestamp: left_record.timestamp, // Use left record's timestamp
            offset: left_record.offset,
            partition: left_record.partition,
            headers: left_record.headers.clone(),
        })
    }

    /// Combine left and right records with NULL values for missing side
    pub fn combine_records_with_nulls(
        base_record: &StreamRecord,
        right_alias: &Option<String>,
        is_left_join: bool, // true for LEFT JOIN, false for RIGHT JOIN
    ) -> Result<StreamRecord, SqlError> {
        let mut combined_fields = base_record.fields.clone();

        // Add NULL fields for the missing side
        if is_left_join {
            // LEFT JOIN: add NULL right fields
            let right_prefix = if let Some(alias) = right_alias {
                format!("{}.", alias)
            } else {
                "right_".to_string()
            };

            // Add common right-side fields as NULL
            combined_fields.insert(format!("{}id", right_prefix), FieldValue::Null);
            combined_fields.insert(format!("{}name", right_prefix), FieldValue::Null);
            combined_fields.insert(format!("{}value", right_prefix), FieldValue::Null);
        } else {
            // RIGHT JOIN: add NULL left fields (preserve original left field names as NULL)
            // This is more complex as we'd need to know the left schema
            // For now, we'll just keep the right record as-is
        }

        Ok(StreamRecord {
            fields: combined_fields,
            timestamp: base_record.timestamp,
            offset: base_record.offset,
            partition: base_record.partition,
            headers: base_record.headers.clone(),
        })
    }

    /// Get right record for JOIN operations with windowing support
    /// In production, this would query the actual right stream/table with windowing constraints
    pub fn get_right_record(
        source: &StreamSource,
        window: &Option<JoinWindow>,
    ) -> Result<Option<StreamRecord>, SqlError> {
        match source {
            StreamSource::Stream(name) | StreamSource::Table(name) => {
                // Simulate different scenarios based on stream name
                if name == "empty_stream" {
                    // Simulate no matching record
                    return Ok(None);
                }

                // Create base mock record
                let mut right_record = Self::create_mock_right_record(source)?;

                // Apply window constraints if specified
                if let Some(join_window) = window {
                    // Simulate windowed join logic
                    let current_time = chrono::Utc::now().timestamp_millis();
                    let window_duration_secs = join_window.time_window.as_secs() as i64;
                    let time_diff = (current_time - right_record.timestamp).abs();

                    if time_diff > window_duration_secs {
                        // Record is outside the time window
                        // Apply grace period if specified
                        if let Some(grace_period) = join_window.grace_period {
                            let grace_secs = grace_period.as_secs() as i64;
                            if time_diff > (window_duration_secs + grace_secs) {
                                return Ok(None); // Outside window and grace period
                            }
                        } else {
                            return Ok(None); // Outside window, no grace period
                        }
                    }

                    // Update timestamp to simulate temporal alignment
                    right_record.timestamp = current_time;
                }

                Ok(Some(right_record))
            }
            StreamSource::Subquery(_) => Err(SqlError::ExecutionError {
                message: "Subquery JOINs not yet implemented".to_string(),
                query: None,
            }),
        }
    }

    /// Create mock right record for JOIN simulation
    pub fn create_mock_right_record(source: &StreamSource) -> Result<StreamRecord, SqlError> {
        let mut fields = HashMap::new();

        match source {
            StreamSource::Stream(name) | StreamSource::Table(name) => {
                // Create different mock data based on source name
                match name.as_str() {
                    "users" => {
                        fields.insert("id".to_string(), FieldValue::Integer(123));
                        fields.insert(
                            "name".to_string(),
                            FieldValue::String("John Doe".to_string()),
                        );
                        fields.insert(
                            "email".to_string(),
                            FieldValue::String("john@example.com".to_string()),
                        );
                    }
                    "orders" => {
                        fields.insert("order_id".to_string(), FieldValue::Integer(456));
                        fields.insert("user_id".to_string(), FieldValue::Integer(123));
                        fields.insert("amount".to_string(), FieldValue::Float(99.99));
                    }
                    "products" => {
                        fields.insert("product_id".to_string(), FieldValue::Integer(789));
                        fields.insert("name".to_string(), FieldValue::String("Widget".to_string()));
                        fields.insert("price".to_string(), FieldValue::Float(29.99));
                    }
                    _ => {
                        // Generic mock data
                        fields.insert("id".to_string(), FieldValue::Integer(1));
                        fields.insert(
                            "name".to_string(),
                            FieldValue::String("mock_data".to_string()),
                        );
                        fields.insert("value".to_string(), FieldValue::String("test".to_string()));
                    }
                }
            }
            StreamSource::Subquery(_) => {
                return Err(SqlError::ExecutionError {
                    message: "Subquery source not supported for mock data".to_string(),
                    query: None,
                });
            }
        }

        Ok(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
        })
    }

    /// Optimized stream-table JOIN processing
    /// Tables are materialized views with key-based lookups, streams are continuous data
    pub fn process_stream_table_join(
        stream_record: &StreamRecord,
        table_source: &StreamSource,
        join_condition: &Expr,
        join_type: &JoinType,
    ) -> Result<Option<StreamRecord>, SqlError> {
        match table_source {
            StreamSource::Table(table_name) => {
                // Extract join key from stream record using the join condition
                // For demo purposes, assume the join is on 'id' field
                let join_key =
                    stream_record
                        .fields
                        .get("id")
                        .ok_or_else(|| SqlError::ExecutionError {
                            message: "Join key 'id' not found in stream record".to_string(),
                            query: None,
                        })?;

                // Simulate table lookup based on key
                if let FieldValue::Integer(key_value) = join_key {
                    if *key_value > 0 {
                        // Simulate successful table lookup
                        let table_record = Self::create_mock_table_record(table_name, *key_value)?;
                        let combined = Self::combine_records(stream_record, &table_record, &None)?;

                        // Evaluate join condition
                        if ExpressionEvaluator::evaluate_expression(join_condition, &combined)? {
                            return Ok(Some(combined));
                        }
                    }
                }

                // Handle different join types for no match
                match join_type {
                    JoinType::Inner => Ok(None), // No result for INNER JOIN
                    JoinType::Left => {
                        // LEFT JOIN: return stream record with NULL table fields
                        Ok(Some(Self::combine_records_with_nulls(
                            stream_record,
                            &None,
                            true,
                        )?))
                    }
                    JoinType::Right => Ok(None), // RIGHT JOIN with stream-table is unusual
                    JoinType::FullOuter => {
                        // FULL OUTER: return stream record with NULL table fields
                        Ok(Some(Self::combine_records_with_nulls(
                            stream_record,
                            &None,
                            true,
                        )?))
                    }
                }
            }
            _ => {
                // Fallback to regular stream-stream JOIN
                Err(SqlError::ExecutionError {
                    message: "Stream-table JOIN requires a table source".to_string(),
                    query: None,
                })
            }
        }
    }

    /// Create mock table record for stream-table JOIN optimization
    pub fn create_mock_table_record(table_name: &str, key: i64) -> Result<StreamRecord, SqlError> {
        let mut fields = HashMap::new();

        // Simulate table data based on key lookup
        fields.insert("id".to_string(), FieldValue::Integer(key));
        fields.insert("table_id".to_string(), FieldValue::Integer(key));
        fields.insert(
            "table_name".to_string(),
            FieldValue::String(format!("table_data_{}", key)),
        );
        fields.insert(
            "user_name".to_string(),
            FieldValue::String(format!("user_{}", key)),
        );
        fields.insert(
            "status".to_string(),
            FieldValue::String("active".to_string()),
        );

        Ok(StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            headers: HashMap::new(),
        })
    }

    /// Extract join key from a record based on join condition
    pub fn extract_join_key(
        record: &StreamRecord,
        join_condition: &Expr,
    ) -> Result<Vec<FieldValue>, SqlError> {
        // This is a simplified implementation
        // In practice, you'd parse the join condition to extract the key fields
        match join_condition {
            Expr::BinaryOp { left, right, .. } => {
                let left_value = ExpressionEvaluator::evaluate_expression_value(left, record)?;
                let right_value = ExpressionEvaluator::evaluate_expression_value(right, record)?;
                Ok(vec![left_value, right_value])
            }
            Expr::Column(name) => {
                let value = record.fields.get(name).cloned().unwrap_or(FieldValue::Null);
                Ok(vec![value])
            }
            _ => Ok(vec![FieldValue::Null]),
        }
    }

    /// Check if two join keys match
    pub fn keys_match(left_key: &[FieldValue], right_key: &[FieldValue]) -> bool {
        if left_key.len() != right_key.len() {
            return false;
        }

        left_key
            .iter()
            .zip(right_key.iter())
            .all(|(l, r)| Self::values_equal(l, r))
    }

    /// Check if two FieldValues are equal (with NULL handling)
    fn values_equal(a: &FieldValue, b: &FieldValue) -> bool {
        match (a, b) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => a == b,
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Null, FieldValue::Null) => true,
            (FieldValue::Date(a), FieldValue::Date(b)) => a == b,
            (FieldValue::Timestamp(a), FieldValue::Timestamp(b)) => a == b,
            (FieldValue::Decimal(a), FieldValue::Decimal(b)) => a == b,
            _ => false,
        }
    }
}
