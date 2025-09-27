//! JOIN Query Processor
//!
//! Handles all types of JOIN operations including INNER, LEFT, RIGHT, and FULL OUTER joins.
//! Includes optimized Stream-Table join support for real-time enrichment.

use super::{ProcessorContext, SelectProcessor};
use super::stream_table_join::StreamTableJoinProcessor;
use crate::velostream::sql::ast::{JoinClause, JoinType, StreamSource};
use crate::velostream::sql::execution::algorithms::{HashJoinBuilder, JoinStrategy};
use crate::velostream::sql::execution::{
    expression::ExpressionEvaluator, FieldValue, StreamRecord,
};
use crate::velostream::sql::SqlError;
use std::collections::HashMap;

/// JOIN processing utilities
pub struct JoinProcessor {
    /// Specialized processor for stream-table joins
    stream_table_processor: StreamTableJoinProcessor,
}

impl JoinProcessor {
    /// Create a new JOIN processor
    pub fn new() -> Self {
        Self {
            stream_table_processor: StreamTableJoinProcessor::new(),
        }
    }

    /// Process all JOIN clauses and combine records
    pub fn process_joins(
        &self,
        left_record: &StreamRecord,
        join_clauses: &[JoinClause],
        context: &mut ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        let mut result_record = left_record.clone();

        for join_clause in join_clauses {
            // Check if this is a stream-table join that can be optimized
            if StreamTableJoinProcessor::is_stream_table_join(
                &StreamSource::Stream("stream".to_string()), // Placeholder - actual source from context
                &join_clause.right_source,
            ) {
                // Use optimized stream-table join
                let joined_records = self
                    .stream_table_processor
                    .process_stream_table_join(&result_record, join_clause, context)?;

                // For single record processing, take the first result
                if let Some(first) = joined_records.into_iter().next() {
                    result_record = first;
                } else if join_clause.join_type == JoinType::Inner {
                    // Inner join with no matches - return error to skip record
                    return Err(SqlError::ExecutionError {
                        message: "No matching records in table for inner join".to_string(),
                        query: None,
                    });
                } else {
                    // Left/Full join - keep the stream record as-is
                }
            } else {
                result_record = Self::process_single_join(&result_record, join_clause, context)?;
            }
        }

        Ok(result_record)
    }

    /// Process batch JOIN with hash join optimization
    pub fn process_batch_joins(
        &self,
        left_records: Vec<StreamRecord>,
        join_clauses: &[JoinClause],
        context: &mut ProcessorContext,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        if join_clauses.is_empty() {
            return Ok(left_records);
        }

        // Check if this is a stream-table join
        let join_clause = &join_clauses[0];
        if StreamTableJoinProcessor::is_stream_table_join(
            &StreamSource::Stream("stream".to_string()),  // Placeholder
            &join_clause.right_source,
        ) {
            // Use optimized batch stream-table join
            return self
                .stream_table_processor
                .process_batch_stream_table_join(left_records, join_clause, context);
        }

        // For now, process first join clause with hash join if beneficial

        // Get right records for the join
        let right_records = Self::get_right_records_batch(&join_clause.right_source, context)?;

        // Determine join strategy based on data sizes
        let strategy = Self::select_join_strategy(left_records.len(), right_records.len());

        match strategy {
            JoinStrategy::HashJoin => {
                Self::execute_hash_join(left_records, right_records, join_clause, context)
            }
            _ => {
                // Fall back to nested loop for small datasets
                Self::execute_nested_loop_batch(left_records, right_records, join_clause, context)
            }
        }
    }

    /// Select optimal join strategy based on cardinalities
    fn select_join_strategy(left_size: usize, right_size: usize) -> JoinStrategy {
        // Use hash join if one side is significantly larger
        // and both sides have reasonable size
        if (left_size > 100 || right_size > 100)
            && (left_size > right_size * 2 || right_size > left_size * 2)
        {
            JoinStrategy::HashJoin
        } else {
            JoinStrategy::NestedLoop
        }
    }

    /// Execute hash join for batch processing
    fn execute_hash_join(
        left_records: Vec<StreamRecord>,
        right_records: Vec<StreamRecord>,
        join_clause: &JoinClause,
        context: &mut ProcessorContext,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        // Build hash join executor
        let mut executor = HashJoinBuilder::new()
            .with_strategy(JoinStrategy::HashJoin)
            .build(join_clause.clone())?;

        // Execute hash join
        executor.execute(left_records, right_records, context)
    }

    /// Execute nested loop join for batch processing
    fn execute_nested_loop_batch(
        left_records: Vec<StreamRecord>,
        right_records: Vec<StreamRecord>,
        join_clause: &JoinClause,
        context: &mut ProcessorContext,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        let mut results = Vec::new();

        for left_record in &left_records {
            for right_record in &right_records {
                match Self::try_join_records(left_record, right_record, join_clause, context) {
                    Ok(combined) => results.push(combined),
                    Err(_) => continue, // Skip non-matching records
                }
            }
        }

        Ok(results)
    }

    /// Try to join two records based on join condition
    fn try_join_records(
        left_record: &StreamRecord,
        right_record: &StreamRecord,
        join_clause: &JoinClause,
        context: &ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        let combined_record =
            Self::combine_records(left_record, right_record, &join_clause.right_alias)?;

        let subquery_executor = SelectProcessor;
        if ExpressionEvaluator::evaluate_expression_with_subqueries(
            &join_clause.condition,
            &combined_record,
            &subquery_executor,
            context,
        )? {
            Ok(combined_record)
        } else {
            Err(SqlError::ExecutionError {
                message: "Join condition not met".to_string(),
                query: None,
            })
        }
    }

    /// Get batch of right records for join
    fn get_right_records_batch(
        _source: &StreamSource,
        _context: &ProcessorContext,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        // Simplified: return empty vector for now
        // In production, fetch records from source
        Ok(Vec::new())
    }

    /// Process a single JOIN clause
    fn process_single_join(
        left_record: &StreamRecord,
        join_clause: &JoinClause,
        context: &mut ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        // Get right record using context-aware method with real data sources
        let right_record_opt = context.join_context.get_right_record_with_context(
            &join_clause.right_source,
            &join_clause.window,
            context,
        )?;

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
                Self::combine_records_with_nulls(
                    left_record,
                    &join_clause.right_source,
                    &join_clause.right_alias,
                    true,
                    context,
                )
            }
        } else {
            // LEFT JOIN: if no right record, use left record with NULL right fields
            Self::combine_records_with_nulls(
                left_record,
                &join_clause.right_source,
                &join_clause.right_alias,
                true,
                context,
            )
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
                // For left nulls, we need to use fallback since we don't have left source info
                Self::combine_records_with_nulls_fallback(&right_record, None, false)
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
                Self::combine_records_with_nulls(
                    left_record,
                    &join_clause.right_source,
                    &join_clause.right_alias,
                    true,
                    context,
                )
            }
        } else {
            // FULL OUTER JOIN: if no right record, use left record with NULL right fields
            Self::combine_records_with_nulls(
                left_record,
                &join_clause.right_source,
                &join_clause.right_alias,
                true,
                context,
            )
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
            event_time: None,
        })
    }

    /// Combine records with NULL values for failed JOIN conditions
    fn combine_records_with_nulls(
        base_record: &StreamRecord,
        source: &StreamSource,
        alias: &Option<String>,
        add_right_nulls: bool,
        context: &ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        let mut combined_fields = base_record.fields.clone();

        if add_right_nulls {
            // Add NULL fields for right side using actual schema information
            let right_null_fields = Self::create_null_fields_for_source(source, alias, context);
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
            event_time: None,
        })
    }

    /// Create NULL fields for a given source using actual schema information
    fn create_null_fields_for_source(
        source: &StreamSource,
        alias: &Option<String>,
        context: &ProcessorContext,
    ) -> HashMap<String, FieldValue> {
        let mut null_fields = HashMap::new();

        // Extract the source name from StreamSource
        let source_name = match source {
            StreamSource::Stream(name) => name,
            StreamSource::Table(name) => name,
            StreamSource::Uri(uri) => uri,
            StreamSource::Subquery(_) => {
                // For subqueries, we don't have a direct schema lookup
                // Fall back to creating common fields
                return Self::create_null_fields_fallback(alias);
            }
        };

        // Look up the actual schema for this source
        if let Some(schema) = context.schemas.get(source_name) {
            // Create null fields based on actual schema field definitions
            for field_def in &schema.fields {
                let final_key = if let Some(alias_str) = alias {
                    format!("{}.{}", alias_str, field_def.name)
                } else {
                    field_def.name.clone()
                };
                null_fields.insert(final_key, FieldValue::Null);
            }
        } else {
            // Schema not found - fall back to common fields or try data source inspection
            if let Some(data_records) = context.data_sources.get(source_name) {
                if let Some(sample_record) = data_records.first() {
                    // Create null fields based on sample record structure
                    for field_name in sample_record.fields.keys() {
                        let final_key = if let Some(alias_str) = alias {
                            format!("{}.{}", alias_str, field_name)
                        } else {
                            field_name.clone()
                        };
                        null_fields.insert(final_key, FieldValue::Null);
                    }
                } else {
                    // No data available - use fallback
                    return Self::create_null_fields_fallback(alias);
                }
            } else {
                // No schema or data available - use fallback
                return Self::create_null_fields_fallback(alias);
            }
        }

        null_fields
    }

    /// Fallback method to create common NULL fields when schema is unavailable
    fn create_null_fields_fallback(alias: &Option<String>) -> HashMap<String, FieldValue> {
        let mut null_fields = HashMap::new();

        // Common fields used as fallback
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

    /// Fallback method for combine_records_with_nulls when source info is unavailable
    fn combine_records_with_nulls_fallback(
        base_record: &StreamRecord,
        alias: Option<String>,
        add_right_nulls: bool,
    ) -> Result<StreamRecord, SqlError> {
        let mut combined_fields = base_record.fields.clone();

        if add_right_nulls {
            // Add NULL fields for right side using fallback
            let right_null_fields =
                Self::create_null_fields_fallback(&Some(alias.unwrap_or("right".to_string())));
            combined_fields.extend(right_null_fields);
        } else {
            // Add NULL fields for left side using fallback
            let left_null_fields =
                Self::create_null_fields_fallback(&Some(alias.unwrap_or("left".to_string())));
            combined_fields.extend(left_null_fields);
        }

        Ok(StreamRecord {
            fields: combined_fields,
            timestamp: base_record.timestamp,
            offset: base_record.offset,
            partition: base_record.partition,
            headers: base_record.headers.clone(),
            event_time: None,
        })
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

    /// Execute a subquery for JOIN operations, returning an optional StreamRecord
    pub fn execute_subquery_for_join(
        subquery: &crate::velostream::sql::StreamingQuery,
        context: &ProcessorContext,
    ) -> Result<Option<StreamRecord>, SqlError> {
        use crate::velostream::sql::ast::StreamingQuery;

        // Execute the subquery against available data sources
        match subquery {
            StreamingQuery::Select { .. } => {
                // Get data source for the subquery
                if let Some(data_sources) =
                    Self::get_subquery_data_source_for_join(subquery, context)
                {
                    // For JOIN operations, return the first matching record
                    // In a full implementation, this would apply WHERE conditions,
                    // GROUP BY, and other query clauses
                    if let Some(first_record) = data_sources.first() {
                        Ok(Some(first_record.clone()))
                    } else {
                        Ok(None)
                    }
                } else {
                    // No data source available - this is expected for some test scenarios
                    // or when the subquery references tables not available in context
                    Ok(None)
                }
            }
            _ => {
                // Non-SELECT subqueries are not supported in JOIN context
                Err(SqlError::ExecutionError {
                    message: format!(
                        "Unsupported subquery type in JOIN operation: {:?}",
                        subquery
                    ),
                    query: None,
                })
            }
        }
    }

    /// Get data source for subquery execution in JOIN context
    fn get_subquery_data_source_for_join(
        query: &crate::velostream::sql::StreamingQuery,
        context: &ProcessorContext,
    ) -> Option<Vec<StreamRecord>> {
        use crate::velostream::sql::ast::{StreamSource, StreamingQuery};

        match query {
            StreamingQuery::Select { from, .. } => {
                // Extract table/stream name from StreamSource
                let source_name = match from {
                    StreamSource::Table(name) => Some(name),
                    StreamSource::Stream(name) => Some(name),
                    StreamSource::Uri(uri) => Some(uri),
                    StreamSource::Subquery(_) => {
                        // Nested subqueries would require recursive execution
                        // For now, not supported
                        None
                    }
                };

                if let Some(name) = source_name {
                    context.data_sources.get(name).cloned()
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
