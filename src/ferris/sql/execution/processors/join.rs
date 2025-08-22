//! JOIN Query Processor
//!
//! Handles all types of JOIN operations including INNER, LEFT, RIGHT, and FULL OUTER joins.

use super::{ProcessorContext, SelectProcessor};
use crate::ferris::sql::SqlError;
use crate::ferris::sql::ast::{JoinClause, JoinType, StreamSource};
use crate::ferris::sql::execution::{FieldValue, StreamRecord, expression::ExpressionEvaluator};
use std::collections::HashMap;

/// JOIN processing utilities
pub struct JoinProcessor;

impl JoinProcessor {
    /// Process all JOIN clauses and combine records
    pub fn process_joins(
        left_record: &StreamRecord,
        join_clauses: &[JoinClause],
        context: &mut ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        let mut result_record = left_record.clone();

        for join_clause in join_clauses {
            result_record = Self::process_single_join(&result_record, join_clause, context)?;
        }

        Ok(result_record)
    }

    /// Process a single JOIN clause
    fn process_single_join(
        left_record: &StreamRecord,
        join_clause: &JoinClause,
        context: &mut ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        // Get right record - in production this would query actual data sources
        let right_record_opt = context
            .join_context
            .get_right_record(&join_clause.right_source, &join_clause.window)?;

        match join_clause.join_type {
            JoinType::Inner => {
                Self::process_inner_join(left_record, right_record_opt, join_clause, context)
            }
            JoinType::Left => {
                Self::process_left_join(left_record, right_record_opt, join_clause, context)
            }
            JoinType::Right => {
                Self::process_right_join(left_record, right_record_opt, join_clause, context)
            }
            JoinType::FullOuter => {
                Self::process_full_outer_join(left_record, right_record_opt, join_clause, context)
            }
        }
    }

    /// Process INNER JOIN
    fn process_inner_join(
        left_record: &StreamRecord,
        right_record_opt: Option<StreamRecord>,
        join_clause: &JoinClause,
        context: &ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        if let Some(right_record) = right_record_opt {
            let combined_record =
                Self::combine_records(left_record, &right_record, &join_clause.right_alias)?;

            // Create a SelectProcessor instance for subquery evaluation in JOIN conditions
            let subquery_executor = SelectProcessor;
            if ExpressionEvaluator::evaluate_expression_with_subqueries(
                &join_clause.condition,
                &combined_record,
                &subquery_executor,
                context,
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

    /// Process LEFT JOIN
    fn process_left_join(
        left_record: &StreamRecord,
        right_record_opt: Option<StreamRecord>,
        join_clause: &JoinClause,
        context: &ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        if let Some(right_record) = right_record_opt {
            let combined_record =
                Self::combine_records(left_record, &right_record, &join_clause.right_alias)?;

            // Create a SelectProcessor instance for subquery evaluation in JOIN conditions
            let subquery_executor = SelectProcessor;
            if ExpressionEvaluator::evaluate_expression_with_subqueries(
                &join_clause.condition,
                &combined_record,
                &subquery_executor,
                context,
            )? {
                Ok(combined_record)
            } else {
                // LEFT JOIN: if condition fails, use left record with NULL right fields
                Self::combine_records_with_nulls(left_record, &join_clause.right_alias, true)
            }
        } else {
            // LEFT JOIN: if no right record, use left record with NULL right fields
            Self::combine_records_with_nulls(left_record, &join_clause.right_alias, true)
        }
    }

    /// Process RIGHT JOIN
    fn process_right_join(
        left_record: &StreamRecord,
        right_record_opt: Option<StreamRecord>,
        join_clause: &JoinClause,
        context: &ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        if let Some(right_record) = right_record_opt {
            let combined_record =
                Self::combine_records(left_record, &right_record, &join_clause.right_alias)?;

            // Create a SelectProcessor instance for subquery evaluation in JOIN conditions
            let subquery_executor = SelectProcessor;
            if ExpressionEvaluator::evaluate_expression_with_subqueries(
                &join_clause.condition,
                &combined_record,
                &subquery_executor,
                context,
            )? {
                Ok(combined_record)
            } else {
                // RIGHT JOIN: if condition fails, use right record with NULL left fields
                Self::combine_records_with_nulls(&right_record, &join_clause.right_alias, false)
            }
        } else {
            // RIGHT JOIN: if no right record, no result (this scenario is rare in stream processing)
            Err(SqlError::ExecutionError {
                message: "No right record for RIGHT JOIN".to_string(),
                query: None,
            })
        }
    }

    /// Process FULL OUTER JOIN
    fn process_full_outer_join(
        left_record: &StreamRecord,
        right_record_opt: Option<StreamRecord>,
        join_clause: &JoinClause,
        context: &ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        if let Some(right_record) = right_record_opt {
            let combined_record =
                Self::combine_records(left_record, &right_record, &join_clause.right_alias)?;

            // Create a SelectProcessor instance for subquery evaluation in JOIN conditions
            let subquery_executor = SelectProcessor;
            if ExpressionEvaluator::evaluate_expression_with_subqueries(
                &join_clause.condition,
                &combined_record,
                &subquery_executor,
                context,
            )? {
                Ok(combined_record)
            } else {
                // FULL OUTER JOIN: if condition fails, could return both records with NULLs
                // For simplicity, we'll return left record with NULL right fields
                Self::combine_records_with_nulls(left_record, &join_clause.right_alias, true)
            }
        } else {
            // FULL OUTER JOIN: if no right record, use left record with NULL right fields
            Self::combine_records_with_nulls(left_record, &join_clause.right_alias, true)
        }
    }

    /// Combine two records for JOIN operations
    fn combine_records(
        left_record: &StreamRecord,
        right_record: &StreamRecord,
        right_alias: &Option<String>,
    ) -> Result<StreamRecord, SqlError> {
        let mut combined_fields = left_record.fields.clone();

        // Add right record fields with optional aliasing
        for (key, value) in &right_record.fields {
            let final_key = if let Some(alias) = right_alias {
                format!("{}.{}", alias, key)
            } else {
                key.clone()
            };
            combined_fields.insert(final_key, value.clone());
        }

        Ok(StreamRecord {
            fields: combined_fields,
            timestamp: left_record.timestamp.max(right_record.timestamp),
            offset: left_record.offset,
            partition: left_record.partition,
            headers: Self::merge_headers(&left_record.headers, &right_record.headers),
        })
    }

    /// Combine records with NULL values for failed JOIN conditions
    fn combine_records_with_nulls(
        base_record: &StreamRecord,
        alias: &Option<String>,
        add_right_nulls: bool,
    ) -> Result<StreamRecord, SqlError> {
        let mut combined_fields = base_record.fields.clone();

        if add_right_nulls {
            // Add NULL fields for right side (simulated)
            let right_null_fields = Self::create_null_fields_for_alias(alias);
            combined_fields.extend(right_null_fields);
        } else {
            // Add NULL fields for left side (less common)
            // This would need actual field names from left side schema
            // For now, we'll just return the base record
        }

        Ok(StreamRecord {
            fields: combined_fields,
            timestamp: base_record.timestamp,
            offset: base_record.offset,
            partition: base_record.partition,
            headers: base_record.headers.clone(),
        })
    }

    /// Create NULL fields for a given alias (simplified implementation)
    fn create_null_fields_for_alias(alias: &Option<String>) -> HashMap<String, FieldValue> {
        let mut null_fields = HashMap::new();

        // In a real implementation, this would query the schema for field names
        // For now, we'll create some common NULL fields
        let common_fields = vec!["id", "name", "value", "amount", "timestamp"];

        for field in common_fields {
            let final_key = if let Some(alias_str) = alias {
                format!("{}.{}", alias_str, field)
            } else {
                field.to_string()
            };
            null_fields.insert(final_key, FieldValue::Null);
        }

        null_fields
    }

    /// Merge headers from left and right records
    fn merge_headers(
        left_headers: &HashMap<String, String>,
        right_headers: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut merged = left_headers.clone();

        // Add right headers, with right taking precedence for conflicts
        for (key, value) in right_headers {
            merged.insert(key.clone(), value.clone());
        }

        merged
    }

    /// Execute a subquery to get right side record for JOIN operations
    pub fn execute_subquery_for_join(
        _subquery: &crate::ferris::sql::StreamingQuery,
    ) -> Result<Option<StreamRecord>, SqlError> {
        Err(SqlError::ExecutionError {
            message: "Subquery execution in JOIN operations is not yet implemented. This requires executing the subquery against actual data and joining the results.".to_string(),
            query: None,
        })
    }

    /// Get mock right record for testing/simulation (moved from engine)
    pub fn create_mock_right_record(source: &StreamSource) -> Result<StreamRecord, SqlError> {
        match source {
            StreamSource::Stream(_name) => {
                Err(SqlError::ExecutionError {
                    message: "Stream-to-stream JOIN operations are not yet implemented. This requires multi-stream coordination, windowing, and state management.".to_string(),
                    query: None,
                })
            }
            StreamSource::Table(_name) => {
                Err(SqlError::ExecutionError {
                    message: "Stream-to-table JOIN operations are not yet implemented. This requires materialized table state and key-based lookups.".to_string(),
                    query: None,
                })
            }
            StreamSource::Subquery(_) => {
                Err(SqlError::ExecutionError {
                    message: "Subquery JOINs are not yet implemented. This requires executing subqueries and joining their results.".to_string(),
                    query: None,
                })
            }
        }
    }
}
