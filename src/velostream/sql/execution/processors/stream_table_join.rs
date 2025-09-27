//! Stream-Table JOIN Processor
//!
//! Optimized JOIN processor for stream-table patterns commonly used in financial services.
//! Provides high-performance lookups for enriching streaming data with reference tables.

use super::{ProcessorContext, SelectProcessor};
use crate::velostream::sql::ast::{BinaryOperator, Expr, JoinClause, JoinType, LiteralValue, StreamSource};
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::{FieldValue, StreamRecord};
use crate::velostream::sql::SqlError;
use crate::velostream::table::{OptimizedTableImpl, UnifiedTable};
use std::collections::HashMap;
use std::sync::Arc;

/// Stream-Table JOIN processor for real-time enrichment
pub struct StreamTableJoinProcessor {
    /// Expression evaluator for JOIN conditions
    evaluator: ExpressionEvaluator,
}

impl StreamTableJoinProcessor {
    /// Create a new Stream-Table JOIN processor
    pub fn new() -> Self {
        Self {
            evaluator: ExpressionEvaluator::new(),
        }
    }

    /// Check if a join is a stream-table join pattern
    pub fn is_stream_table_join(
        left_source: &StreamSource,
        right_source: &StreamSource,
    ) -> bool {
        match (left_source, right_source) {
            (StreamSource::Stream(_), StreamSource::Table(_)) => true,
            (StreamSource::Table(_), StreamSource::Stream(_)) => true,
            _ => false,
        }
    }

    /// Process a stream-table JOIN with optimized table lookup
    pub fn process_stream_table_join(
        &self,
        stream_record: &StreamRecord,
        join_clause: &JoinClause,
        context: &mut ProcessorContext,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        // Identify which side is the table
        let table_name = match &join_clause.right_source {
            StreamSource::Table(name) => name.clone(),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "Stream-Table join requires a table on the right side".to_string(),
                    query: None,
                })
            }
        };

        // Get the table from context
        let table = self.get_table_from_context(&table_name, context)?;

        // Extract join key from the stream record based on JOIN condition
        let join_keys = self.extract_join_keys(&join_clause.condition, stream_record)?;

        // Perform optimized table lookup(s)
        let table_records = self.lookup_table_records(&table, &join_keys, &join_clause.condition)?;

        // Combine stream record with matching table records
        self.combine_stream_table_records(
            stream_record,
            table_records,
            &join_clause.join_type,
            join_clause.right_alias.as_deref(),
        )
    }

    /// Batch process stream-table JOINs for multiple stream records
    pub fn process_batch_stream_table_join(
        &self,
        stream_records: Vec<StreamRecord>,
        join_clause: &JoinClause,
        context: &mut ProcessorContext,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        // Identify table
        let table_name = match &join_clause.right_source {
            StreamSource::Table(name) => name.clone(),
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "Stream-Table join requires a table on the right side".to_string(),
                    query: None,
                })
            }
        };

        // Get the table
        let table = self.get_table_from_context(&table_name, context)?;

        // Collect all join keys from stream records for batch lookup
        let mut all_join_keys = Vec::new();
        for record in &stream_records {
            let keys = self.extract_join_keys(&join_clause.condition, record)?;
            all_join_keys.push(keys);
        }

        // Perform batch table lookup (future optimization: single query)
        let mut results = Vec::new();
        for (stream_record, join_keys) in stream_records.iter().zip(all_join_keys.iter()) {
            let table_records = self.lookup_table_records(&table, join_keys, &join_clause.condition)?;
            let joined = self.combine_stream_table_records(
                stream_record,
                table_records,
                &join_clause.join_type,
                join_clause.right_alias.as_deref(),
            )?;
            results.extend(joined);
        }

        Ok(results)
    }

    /// Get table from processor context
    fn get_table_from_context(
        &self,
        table_name: &str,
        context: &ProcessorContext,
    ) -> Result<Arc<dyn UnifiedTable>, SqlError> {
        // Try to get table from context's table registry
        context.get_table(table_name).map_err(|original_error| {
            SqlError::ExecutionError {
                message: format!(
                    "Table '{}' not found in context for Stream-Table join. Original error: {}",
                    table_name, original_error
                ),
                query: None,
            }
        })
    }

    /// Extract join keys from JOIN condition
    fn extract_join_keys(
        &self,
        condition: &Expr,
        stream_record: &StreamRecord,
    ) -> Result<HashMap<String, FieldValue>, SqlError> {
        let mut join_keys = HashMap::new();

        // Parse JOIN condition to extract key fields
        // Common pattern: t.user_id = u.user_id
        match condition {
            Expr::BinaryOp { op, left, right } if *op == BinaryOperator::Equal => {
                // Check if this is a field equality
                if let (Expr::Column(left_field), Expr::Column(right_field)) =
                    (left.as_ref(), right.as_ref())
                {
                    // Extract value from stream record
                    if let Some(value) = stream_record.fields.get(left_field) {
                        join_keys.insert(right_field.clone(), value.clone());
                    } else if let Some(value) = stream_record.fields.get(right_field) {
                        join_keys.insert(left_field.clone(), value.clone());
                    } else {
                        // Neither field found in stream record - this is an error for stream-table joins
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "JOIN condition references fields '{}' and '{}' but neither is present in the stream record. Available fields: {:?}",
                                left_field, right_field, stream_record.fields.keys().collect::<Vec<_>>()
                            ),
                            query: None,
                        });
                    }
                } else {
                    // Non-column expressions in equality condition
                    return Err(SqlError::ExecutionError {
                        message: format!(
                            "Stream-Table JOIN equality condition must compare two column references. Found: {:?} = {:?}",
                            left, right
                        ),
                        query: None,
                    });
                }
            }
            Expr::BinaryOp { op, left, right } if *op == BinaryOperator::And => {
                // Handle multiple JOIN conditions
                let left_keys = self.extract_join_keys(left, stream_record)?;
                let right_keys = self.extract_join_keys(right, stream_record)?;
                join_keys.extend(left_keys);
                join_keys.extend(right_keys);
            }
            Expr::BinaryOp { op, left, right } => {
                // Other binary operators not supported for Stream-Table joins
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "Unsupported binary operator '{:?}' in Stream-Table JOIN condition. Only '=' and 'AND' are supported. Found: {:?} {:?} {:?}",
                        op, left, op, right
                    ),
                    query: None,
                });
            }
            _ => {
                // Complex condition not supported for Stream-Table joins
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "Unsupported JOIN condition for Stream-Table join. Only equality conditions and AND combinations are supported. Found: {:?}",
                        condition
                    ),
                    query: None,
                });
            }
        }

        // Validate that we extracted at least one join key
        if join_keys.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "No valid join keys could be extracted from JOIN condition: {:?}. Ensure the condition references fields present in the stream record.",
                    condition
                ),
                query: None,
            });
        }

        Ok(join_keys)
    }

    /// Lookup records from table using join keys
    fn lookup_table_records(
        &self,
        table: &Arc<dyn UnifiedTable>,
        join_keys: &HashMap<String, FieldValue>,
        condition: &Expr,
    ) -> Result<Vec<HashMap<String, FieldValue>>, SqlError> {
        let mut matching_records = Vec::new();

        // Iterate through all table records to find matches
        for (_key, record) in table.iter_records() {
            let mut matches = true;

            // Check if this record matches all join key conditions
            for (field_name, required_value) in join_keys {
                if let Some(record_value) = record.get(field_name) {
                    if record_value != required_value {
                        matches = false;
                        break;
                    }
                } else {
                    // Field doesn't exist in table record
                    matches = false;
                    break;
                }
            }

            if matches {
                matching_records.push(record);
            }
        }

        Ok(matching_records)
    }

    /// Combine stream record with table records based on JOIN type
    fn combine_stream_table_records(
        &self,
        stream_record: &StreamRecord,
        table_records: Vec<HashMap<String, FieldValue>>,
        join_type: &JoinType,
        table_alias: Option<&str>,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        let mut results = Vec::new();

        match join_type {
            JoinType::Inner => {
                // INNER JOIN: Only emit when there's a match
                for table_record in table_records {
                    let mut combined = stream_record.clone();
                    // Add table fields with optional alias prefix
                    for (key, value) in table_record {
                        let field_name = if let Some(alias) = table_alias {
                            format!("{}.{}", alias, key)
                        } else {
                            key
                        };
                        combined.fields.insert(field_name, value);
                    }
                    results.push(combined);
                }
            }
            JoinType::Left => {
                // LEFT JOIN: Always emit stream record, with NULLs if no match
                if table_records.is_empty() {
                    // No match - emit with NULLs for table fields
                    results.push(stream_record.clone());
                } else {
                    // Emit combined records for each match
                    for table_record in table_records {
                        let mut combined = stream_record.clone();
                        for (key, value) in table_record {
                            let field_name = if let Some(alias) = table_alias {
                                format!("{}.{}", alias, key)
                            } else {
                                key
                            };
                            combined.fields.insert(field_name, value);
                        }
                        results.push(combined);
                    }
                }
            }
            JoinType::Right => {
                // RIGHT JOIN: Less common for stream-table, but supported
                // This would emit all table records with matching stream records
                // For stream-table joins, this is unusual but we support it
                if table_records.is_empty() {
                    // No table records - nothing to emit for RIGHT JOIN
                } else {
                    for table_record in table_records {
                        let mut combined = StreamRecord {
                            timestamp: stream_record.timestamp,
                            offset: stream_record.offset,
                            partition: stream_record.partition,
                            fields: table_record,
                            headers: stream_record.headers.clone(),
                            event_time: stream_record.event_time,
                        };
                        // Overlay stream fields
                        combined.fields.extend(stream_record.fields.clone());
                        results.push(combined);
                    }
                }
            }
            JoinType::FullOuter => {
                // FULL OUTER JOIN: Emit all records from both sides
                // This is complex for stream-table and rarely used
                if table_records.is_empty() {
                    // No match - emit stream record with NULLs
                    results.push(stream_record.clone());
                } else {
                    // Emit combined records
                    for table_record in table_records {
                        let mut combined = stream_record.clone();
                        for (key, value) in table_record {
                            let field_name = if let Some(alias) = table_alias {
                                format!("{}.{}", alias, key)
                            } else {
                                key
                            };
                            combined.fields.insert(field_name, value);
                        }
                        results.push(combined);
                    }
                }
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_is_stream_table_join() {
        let processor = StreamTableJoinProcessor::new();

        // Stream-Table join
        assert!(StreamTableJoinProcessor::is_stream_table_join(
            &StreamSource::Stream("trades".to_string()),
            &StreamSource::Table("users".to_string())
        ));

        // Table-Stream join (reversed)
        assert!(StreamTableJoinProcessor::is_stream_table_join(
            &StreamSource::Table("users".to_string()),
            &StreamSource::Stream("trades".to_string())
        ));

        // Not a stream-table join
        assert!(!StreamTableJoinProcessor::is_stream_table_join(
            &StreamSource::Stream("trades".to_string()),
            &StreamSource::Stream("orders".to_string())
        ));
    }

    #[test]
    fn test_extract_join_keys() {
        let processor = StreamTableJoinProcessor::new();

        // Create a test stream record
        let mut fields = HashMap::new();
        fields.insert("user_id".to_string(), FieldValue::Integer(123));
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));

        let stream_record = StreamRecord {
            timestamp: Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            fields,
            headers: HashMap::new(),
            event_time: Some(Utc::now()),
        };

        // Test simple equality condition
        let condition = Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("id".to_string())),
        };

        let keys = processor.extract_join_keys(&condition, &stream_record).unwrap();
        assert_eq!(keys.get("id"), Some(&FieldValue::Integer(123)));

        // Test AND condition with multiple keys
        let multi_condition = Expr::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Column("user_id".to_string())),
                right: Box::new(Expr::Column("id".to_string())),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Column("symbol".to_string())),
                right: Box::new(Expr::Column("stock_symbol".to_string())),
            }),
        };

        let multi_keys = processor.extract_join_keys(&multi_condition, &stream_record).unwrap();
        assert_eq!(multi_keys.get("id"), Some(&FieldValue::Integer(123)));
        assert_eq!(
            multi_keys.get("stock_symbol"),
            Some(&FieldValue::String("AAPL".to_string()))
        );
    }
}