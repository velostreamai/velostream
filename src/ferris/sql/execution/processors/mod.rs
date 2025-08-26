//! Query Processing Modules
//!
//! This module contains specialized processors for different types of SQL operations:
//! - SELECT processing
//! - Window processing
//! - JOIN processing
//! - LIMIT processing
//! - SHOW/DESCRIBE processing

use crate::ferris::sql::execution::expression::evaluator::ExpressionEvaluator;
use crate::ferris::sql::execution::internal::GroupByState;
use crate::ferris::sql::execution::performance::PerformanceMonitor;
use crate::ferris::sql::execution::types::FieldValue;
use crate::ferris::sql::execution::StreamRecord;
use crate::ferris::sql::{SqlError, StreamingQuery};

pub mod context;
pub mod processor_types;

pub use context::{ProcessorContext, WindowContext};
pub use processor_types::{HeaderMutation, HeaderOperation, ProcessorResult};

/// Main processor coordination interface
pub struct QueryProcessor;

impl QueryProcessor {
    /// Process a query against a record using the appropriate processor
    pub fn process_query(
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<ProcessorResult, SqlError> {
        match query {
            StreamingQuery::Select {
                fields,
                from: _,
                where_clause: _,
                group_by,
                having,
                limit,
                ..
            } => {
                // Check LIMIT constraints first
                if let Some(limit_count) = limit {
                    if context.record_count >= *limit_count as u64 {
                        // Already processed enough records, skip this one
                        return Ok(ProcessorResult {
                            record: None,
                            header_mutations: Vec::new(),
                            should_count: false,
                        });
                    }
                }

                // Check if this is a GROUP BY query
                if let Some(group_expressions) = group_by {
                    return Self::process_group_by_query(fields, group_expressions, having, record, context);
                }

                // Process the SELECT fields (simplified implementation)
                let mut result_fields = std::collections::HashMap::new();
                
                // For SELECT *, include all fields
                if fields.iter().any(|f| matches!(f, crate::ferris::sql::ast::SelectField::Wildcard)) {
                    result_fields = record.fields.clone();
                } else {
                    // Process specific fields
                    for field in fields {
                        match field {
                            crate::ferris::sql::ast::SelectField::Column(name) => {
                                if let Some(value) = record.fields.get(name) {
                                    result_fields.insert(name.clone(), value.clone());
                                } else if name.starts_with('_') {
                                    // Handle system columns (case insensitive)
                                    match name.to_lowercase().as_str() {
                                        "_timestamp" => {
                                            result_fields.insert(name.clone(), FieldValue::Integer(record.timestamp));
                                        }
                                        "_offset" => {
                                            result_fields.insert(name.clone(), FieldValue::Integer(record.offset));
                                        }
                                        "_partition" => {
                                            result_fields.insert(name.clone(), FieldValue::Integer(record.partition as i64));
                                        }
                                        _ => {} // Unknown system column, ignore
                                    }
                                }
                            }
                            crate::ferris::sql::ast::SelectField::AliasedColumn { column, alias } => {
                                if let Some(value) = record.fields.get(column) {
                                    result_fields.insert(alias.clone(), value.clone());
                                } else if column.starts_with('_') {
                                    // Handle aliased system columns (case insensitive)
                                    match column.to_lowercase().as_str() {
                                        "_timestamp" => {
                                            result_fields.insert(alias.clone(), FieldValue::Integer(record.timestamp));
                                        }
                                        "_offset" => {
                                            result_fields.insert(alias.clone(), FieldValue::Integer(record.offset));
                                        }
                                        "_partition" => {
                                            result_fields.insert(alias.clone(), FieldValue::Integer(record.partition as i64));
                                        }
                                        _ => {} // Unknown system column, ignore
                                    }
                                }
                            }
                            crate::ferris::sql::ast::SelectField::Expression { expr, alias } => {
                                // Use ExpressionEvaluator to evaluate any expression (columns, literals, functions, etc.)
                                match ExpressionEvaluator::evaluate_expression_value(expr, &record) {
                                    Ok(value) => {
                                        let field_name = if let Some(alias) = alias {
                                            alias.clone()
                                        } else {
                                            // If no alias, try to derive a name from the expression
                                            match expr {
                                                crate::ferris::sql::ast::Expr::Column(name) => name.clone(),
                                                _ => "expr".to_string(), // Default name for non-column expressions
                                            }
                                        };
                                        result_fields.insert(field_name, value);
                                    }
                                    Err(_) => {
                                        // If evaluation fails, skip this field (could add error handling)
                                    }
                                }
                            }
                            _ => {} // Handle other field types as needed
                        }
                    }
                }

                let result_record = Some(StreamRecord {
                    fields: result_fields,
                    timestamp: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                    headers: record.headers.clone(),
                });

                Ok(ProcessorResult {
                    record: result_record,
                    header_mutations: Vec::new(),
                    should_count: true,
                })
            }
            StreamingQuery::CreateStream { as_select, .. } => {
                // For CREATE STREAM AS SELECT, process the inner SELECT query
                Self::process_query(as_select, record, context)
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                // For CREATE TABLE AS SELECT, process the inner SELECT query
                Self::process_query(as_select, record, context)
            }
            _ => {
                // For other query types, use simplified implementation
                Ok(ProcessorResult {
                    record: None,
                    header_mutations: Vec::new(),
                    should_count: true,
                })
            }
        }
    }

    /// Process a query with optional performance monitoring
    pub fn process_query_with_monitoring(
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
        _performance_monitor: Option<&PerformanceMonitor>,
    ) -> Result<ProcessorResult, SqlError> {
        // Use simplified processing for now
        Self::process_query(query, record, context)
    }

    /// Get processor metrics for monitoring
    pub fn get_metrics(context: &ProcessorContext) -> std::collections::HashMap<String, u64> {
        let mut metrics = std::collections::HashMap::new();
        metrics.insert("record_count".to_string(), context.record_count);

        if let Some(max) = context.max_records {
            metrics.insert("max_records".to_string(), max);
        }

        metrics.insert(
            "data_sources".to_string(),
            context.data_sources.len() as u64,
        );
        metrics.insert("schemas".to_string(), context.schemas.len() as u64);
        metrics.insert(
            "stream_handles".to_string(),
            context.stream_handles.len() as u64,
        );

        // Add pluggable data source metrics
        metrics.insert(
            "data_readers".to_string(),
            context.data_readers.len() as u64,
        );
        metrics.insert(
            "data_writers".to_string(),
            context.data_writers.len() as u64,
        );
        metrics.insert(
            "persistent_window_states".to_string(),
            context.persistent_window_states.len() as u64,
        );
        metrics.insert(
            "dirty_window_states_count".to_string(),
            context.dirty_window_states.count_ones() as u64,
        );

        metrics
    }

    /// Clear context state for fresh processing
    pub fn reset_context(context: &mut ProcessorContext) {
        context.record_count = 0;
        context.window_context = None;
        context.data_sources.clear();
    }

    /// Process a GROUP BY query with aggregation
    pub fn process_group_by_query(
        fields: &[crate::ferris::sql::ast::SelectField],
        group_expressions: &[crate::ferris::sql::ast::Expr],
        _having: &Option<crate::ferris::sql::ast::Expr>,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<ProcessorResult, SqlError> {
        // Create a unique key for this GROUP BY query (for now, use a simple key)
        let query_key = "default_group_by".to_string();
        
        // Get or create the GroupByState for this query
        if !context.group_by_states.contains_key(&query_key) {
            let group_by_state = GroupByState::new(
                group_expressions.to_vec(),
                fields.to_vec(),
                _having.clone(),
            );
            context.group_by_states.insert(query_key.clone(), group_by_state);
        }
        
        // Process the record through the GROUP BY state
        let group_by_state = context.group_by_states.get_mut(&query_key).unwrap();
        
        // Compute group key from group expressions
        let mut group_key_values = Vec::new();
        for group_expr in group_expressions {
            match ExpressionEvaluator::evaluate_expression_value(group_expr, record) {
                Ok(value) => group_key_values.push(value),
                Err(_) => return Ok(ProcessorResult {
                    record: None,
                    header_mutations: Vec::new(),
                    should_count: false,
                }),
            }
        }
        
        // Create group key (convert FieldValue to String for the key)
        let group_key: Vec<String> = group_key_values.into_iter()
            .map(|val| format!("{:?}", val))
            .collect();
        
        // Get or create the group accumulator
        let group_accumulator = group_by_state.get_or_create_group(group_key.clone());
        
        // Process the record in this group
        group_accumulator.increment_count();
        group_accumulator.set_sample_record(record.clone());
        
        // Process aggregate functions from the SELECT fields
        for field in fields {
            match field {
                crate::ferris::sql::ast::SelectField::Expression { expr, alias: _ } => {
                    match expr {
                        crate::ferris::sql::ast::Expr::Function { name, args } => {
                            match name.to_lowercase().as_str() {
                                "sum" => {
                                    if let Some(arg) = args.first() {
                                        if let Ok(FieldValue::Float(val)) = ExpressionEvaluator::evaluate_expression_value(arg, record) {
                                            group_accumulator.add_sum("sum_field", val);
                                        } else if let Ok(FieldValue::Integer(val)) = ExpressionEvaluator::evaluate_expression_value(arg, record) {
                                            group_accumulator.add_sum("sum_field", val as f64);
                                        }
                                    }
                                }
                                "count" => {
                                    // COUNT(*) or COUNT(column)
                                    group_accumulator.add_non_null_count("count_field");
                                }
                                "min" => {
                                    if let Some(arg) = args.first() {
                                        if let Ok(val) = ExpressionEvaluator::evaluate_expression_value(arg, record) {
                                            group_accumulator.update_min("min_field", val);
                                        }
                                    }
                                }
                                "max" => {
                                    if let Some(arg) = args.first() {
                                        if let Ok(val) = ExpressionEvaluator::evaluate_expression_value(arg, record) {
                                            group_accumulator.update_max("max_field", val);
                                        }
                                    }
                                }
                                _ => {} // Other functions not implemented yet
                            }
                        }
                        _ => {} // Non-function expressions
                    }
                }
                _ => {} // Other field types
            }
        }
        
        // For streaming GROUP BY, emit the current aggregated state for this group
        
        if let Some(group_accumulator) = group_by_state.get_group(&group_key) {
            let mut result_fields = std::collections::HashMap::new();
            
            // Add group key fields
            for (i, group_expr) in group_expressions.iter().enumerate() {
                if let crate::ferris::sql::ast::Expr::Column(col_name) = group_expr {
                    if let Some(sample_record) = &group_accumulator.sample_record {
                        if let Some(value) = sample_record.fields.get(col_name) {
                            result_fields.insert(col_name.clone(), value.clone());
                        }
                    }
                }
            }
            
            // Add aggregate results
            for field in fields {
                match field {
                    crate::ferris::sql::ast::SelectField::Expression { expr, alias } => {
                        match expr {
                            crate::ferris::sql::ast::Expr::Function { name, args: _ } => {
                                let field_name = alias.as_ref().unwrap_or(name);
                                match name.to_lowercase().as_str() {
                                    "sum" => {
                                        if let Some(sum) = group_accumulator.sums.get("sum_field") {
                                            result_fields.insert(field_name.clone(), FieldValue::Float(*sum));
                                        }
                                    }
                                    "count" => {
                                        result_fields.insert(field_name.clone(), FieldValue::Integer(group_accumulator.count as i64));
                                    }
                                    "min" => {
                                        if let Some(min_val) = group_accumulator.mins.get("min_field") {
                                            result_fields.insert(field_name.clone(), min_val.clone());
                                        }
                                    }
                                    "max" => {
                                        if let Some(max_val) = group_accumulator.maxs.get("max_field") {
                                            result_fields.insert(field_name.clone(), max_val.clone());
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            
            let result_record = Some(StreamRecord {
                fields: result_fields,
                timestamp: record.timestamp,
                offset: record.offset,
                partition: record.partition,
                headers: record.headers.clone(),
            });
            
            Ok(ProcessorResult {
                record: result_record,
                header_mutations: Vec::new(),
                should_count: true,
            })
        } else {
            Ok(ProcessorResult {
                record: None,
                header_mutations: Vec::new(),
                should_count: true,
            })
        }
    }

    /// Validate context readiness for processing
    pub fn validate_context(context: &ProcessorContext) -> Result<(), SqlError> {
        // Validate that we have active data sources if needed
        if context.data_readers.is_empty() && context.data_sources.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "No data sources available in context".to_string(),
                query: None,
            });
        }

        // Validate window states are not corrupted
        if context.persistent_window_states.len() > 32 {
            return Err(SqlError::ExecutionError {
                message: "Too many persistent window states (max 32 supported)".to_string(),
                query: None,
            });
        }

        Ok(())
    }
}

// Re-export join context
pub use self::join_context::JoinContext;

// Re-export processor modules
pub use self::delete::DeleteProcessor;
pub use self::insert::InsertProcessor;
pub use self::join::JoinProcessor;
pub use self::limit::LimitProcessor;
pub use self::select::SelectProcessor;
pub use self::show::ShowProcessor;
pub use self::update::UpdateProcessor;
pub use self::window::WindowProcessor;

// Re-export sub-modules for direct access
pub mod delete;
pub mod insert;
pub mod job;
pub mod join;
pub mod join_context;
pub mod limit;
pub mod select;
pub mod show;
pub mod update;
pub mod window;
