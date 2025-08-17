//! Top-level stream processor for orchestrating query execution
//!
//! This module provides the main StreamProcessor that coordinates all other
//! execution components to process streaming SQL queries.

use std::collections::HashMap;

use crate::ferris::sql::ast::{Expr, SelectField, StreamingQuery};
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::expressions::ExpressionEvaluator;
use crate::ferris::sql::execution::groupby::GroupByProcessor;
use crate::ferris::sql::execution::joins::JoinProcessor;
use crate::ferris::sql::execution::query_planner::{
    ExecutionPlan, ExecutionStates, PlanNode, QueryPlanner,
};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use crate::ferris::sql::execution::windows::WindowProcessor;

/// Result of processing a record through a streaming query
#[derive(Debug, Clone)]
pub enum ProcessingResult {
    /// Record produced as output
    Record(StreamRecord),
    /// No output (filtered out, buffered for window, etc.)
    NoOutput,
    /// Query completed/stopped
    Completed,
}

/// Main stream processor that orchestrates query execution
pub struct StreamProcessor {
    query: StreamingQuery,
    plan: ExecutionPlan,
    states: ExecutionStates,
    records_processed: usize,
    limit_records_emitted: usize,
    limit_reached: bool,
}

impl StreamProcessor {
    /// Create a new stream processor for a query
    pub fn new(query: StreamingQuery) -> Result<Self, SqlError> {
        let plan = QueryPlanner::create_plan(&query)?;
        let states = QueryPlanner::get_required_states(&plan)?;

        Ok(StreamProcessor {
            query,
            plan,
            states,
            records_processed: 0,
            limit_records_emitted: 0,
            limit_reached: false,
        })
    }

    /// Process a single record through the query
    pub fn process_record(&mut self, record: StreamRecord) -> Result<ProcessingResult, SqlError> {
        if self.limit_reached {
            return Ok(ProcessingResult::NoOutput);
        }

        // Clone the plan root to avoid borrowing issues
        let plan_root = self.plan.root.clone();

        // Execute the plan
        let result = self.execute_plan_node(&plan_root, record)?;

        // Increment records_processed only if we're producing output
        if matches!(result, ProcessingResult::Record(_)) {
            self.records_processed += 1;
        }

        Ok(result)
    }

    /// Execute a plan node
    fn execute_plan_node(
        &mut self,
        node: &PlanNode,
        record: StreamRecord,
    ) -> Result<ProcessingResult, SqlError> {
        match node {
            PlanNode::Source { .. } => {
                // Source node just passes the record through
                Ok(ProcessingResult::Record(record))
            }
            PlanNode::Filter { condition, input } => {
                // First execute the input node
                let input_result = self.execute_plan_node(input, record)?;

                match input_result {
                    ProcessingResult::Record(input_record) => {
                        // Apply filter condition
                        if ExpressionEvaluator::evaluate_expression(condition, &input_record)? {
                            Ok(ProcessingResult::Record(input_record))
                        } else {
                            Ok(ProcessingResult::NoOutput)
                        }
                    }
                    other => Ok(other),
                }
            }
            PlanNode::Projection { fields, input } => {
                // First execute the input node
                let input_result = self.execute_plan_node(input, record)?;

                match input_result {
                    ProcessingResult::Record(input_record) => {
                        let projected_record = self.apply_projection(&input_record, fields)?;
                        Ok(ProcessingResult::Record(projected_record))
                    }
                    other => Ok(other),
                }
            }
            PlanNode::Join {
                join_clauses,
                input,
            } => {
                // First execute the input node
                let input_result = self.execute_plan_node(input, record)?;

                match input_result {
                    ProcessingResult::Record(input_record) => {
                        // Apply JOIN operations
                        match JoinProcessor::process_joins(&input_record, join_clauses) {
                            Ok(joined_record) => Ok(ProcessingResult::Record(joined_record)),
                            Err(_) => Ok(ProcessingResult::NoOutput), // JOIN condition not met
                        }
                    }
                    other => Ok(other),
                }
            }
            PlanNode::Window { input, .. } => {
                // First execute the input node
                let input_result = self.execute_plan_node(input, record)?;

                match input_result {
                    ProcessingResult::Record(input_record) => {
                        if let Some(ref mut window_state) = self.states.window_state {
                            // Add record to window
                            WindowProcessor::process_record(window_state, &input_record)?;

                            // Check if window should emit using the record's event time
                            let current_time = WindowProcessor::extract_event_time(
                                &input_record,
                                window_state.get_time_column(),
                            );
                            if WindowProcessor::should_emit_window(window_state, current_time) {
                                // Get records for emission
                                let window_records =
                                    WindowProcessor::get_window_records_for_emission(
                                        window_state,
                                        current_time,
                                    );

                                // Execute aggregation on window records
                                let result_record = WindowProcessor::execute_windowed_aggregation(
                                    &self.query,
                                    &window_records,
                                )?;

                                // Update last emit time
                                window_state.last_emit = current_time;

                                Ok(ProcessingResult::Record(result_record))
                            } else {
                                Ok(ProcessingResult::NoOutput)
                            }
                        } else {
                            Err(SqlError::ExecutionError {
                                message: "Window state not initialized".to_string(),
                                query: None,
                            })
                        }
                    }
                    other => Ok(other),
                }
            }
            PlanNode::GroupBy { input, .. } => {
                // First execute the input node
                let input_result = self.execute_plan_node(input, record)?;

                match input_result {
                    ProcessingResult::Record(input_record) => {
                        if let Some(ref mut group_state) = self.states.group_by_state {
                            // Process record through GROUP BY
                            GroupByProcessor::process_record(group_state, &input_record)?;

                            // For streaming GROUP BY, we typically emit results periodically
                            // or when certain conditions are met. For now, we'll buffer.
                            Ok(ProcessingResult::NoOutput)
                        } else {
                            Err(SqlError::ExecutionError {
                                message: "GROUP BY state not initialized".to_string(),
                                query: None,
                            })
                        }
                    }
                    other => Ok(other),
                }
            }
            PlanNode::Limit { count, input } => {
                // First execute the input node
                let input_result = self.execute_plan_node(input, record)?;

                match input_result {
                    ProcessingResult::Record(input_record) => {
                        if self.limit_records_emitted < *count {
                            self.limit_records_emitted += 1;
                            if self.limit_records_emitted >= *count {
                                self.limit_reached = true;
                            }
                            Ok(ProcessingResult::Record(input_record))
                        } else {
                            Ok(ProcessingResult::NoOutput)
                        }
                    }
                    other => Ok(other),
                }
            }
        }
    }

    /// Apply projection to a record
    fn apply_projection(
        &self,
        record: &StreamRecord,
        fields: &[SelectField],
    ) -> Result<StreamRecord, SqlError> {
        let mut projected_fields = HashMap::new();

        for field in fields {
            match field {
                SelectField::Wildcard => {
                    // Add all fields from the record
                    for (field_name, field_value) in &record.fields {
                        projected_fields.insert(field_name.clone(), field_value.clone());
                    }
                }
                SelectField::Column(name) => {
                    // Add specific column
                    if let Some(value) = record.fields.get(name) {
                        projected_fields.insert(name.clone(), value.clone());
                    } else {
                        projected_fields.insert(name.clone(), FieldValue::Null);
                    }
                }
                SelectField::AliasedColumn { column, alias } => {
                    // Add column with alias
                    if let Some(value) = record.fields.get(column) {
                        projected_fields.insert(alias.clone(), value.clone());
                    } else {
                        projected_fields.insert(alias.clone(), FieldValue::Null);
                    }
                }
                SelectField::Expression { expr, alias } => {
                    // Evaluate expression
                    let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
                    let field_name = alias
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| self.get_expression_name(expr));
                    projected_fields.insert(field_name, value);
                }
            }
        }

        Ok(StreamRecord {
            fields: projected_fields,
            timestamp: record.timestamp,
            offset: record.offset,
            partition: record.partition,
            headers: record.headers.clone(),
        })
    }

    /// Get a readable name for an expression
    fn get_expression_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Function { name, args } => {
                if args.is_empty() {
                    format!("{}()", name)
                } else {
                    format!("{}(...)", name)
                }
            }
            Expr::Literal(_) => "literal".to_string(),
            Expr::BinaryOp { .. } => "binary_op".to_string(),
            Expr::UnaryOp { .. } => "unary_op".to_string(),
            Expr::Case { .. } => "case".to_string(),
            _ => "expression".to_string(),
        }
    }

    /// Get the LIMIT value from the query
    fn get_query_limit(&self) -> Option<usize> {
        match &self.query {
            StreamingQuery::Select { limit, .. } => limit.map(|l| l as usize),
            _ => None,
        }
    }

    /// Emit accumulated GROUP BY results (called periodically or on query end)
    pub fn emit_group_by_results(&mut self) -> Result<Vec<StreamRecord>, SqlError> {
        if let Some(ref mut group_state) = self.states.group_by_state {
            let mut results = Vec::new();

            // Get all group keys and their accumulators
            for group_key in group_state.group_keys() {
                if let Some(accumulator) = group_state.get_accumulator(group_key) {
                    let sample_record = accumulator.sample_record.as_ref();

                    // Get SELECT fields and GROUP BY expressions from query
                    if let StreamingQuery::Select {
                        fields, group_by, ..
                    } = &self.query
                    {
                        let group_exprs = group_by.as_ref().map(|g| g.as_slice()).unwrap_or(&[]);

                        let result_fields = GroupByProcessor::compute_group_result_fields(
                            accumulator,
                            fields,
                            group_exprs,
                            sample_record,
                        )?;

                        let result_record = StreamRecord {
                            fields: result_fields,
                            timestamp: sample_record.map(|r| r.timestamp).unwrap_or(0),
                            offset: sample_record.map(|r| r.offset).unwrap_or(0),
                            partition: sample_record.map(|r| r.partition).unwrap_or(0),
                            headers: HashMap::new(),
                        };

                        results.push(result_record);
                    }
                }
            }

            Ok(results)
        } else {
            Ok(vec![])
        }
    }

    /// Check if the processor has completed (hit limits, etc.)
    pub fn is_completed(&self) -> bool {
        self.limit_reached
    }

    /// Get processing statistics
    pub fn get_stats(&self) -> ProcessingStats {
        ProcessingStats {
            records_processed: self.records_processed,
            is_windowed: self.plan.is_windowed,
            has_aggregation: self.plan.has_aggregation,
            has_joins: self.plan.has_joins,
            estimated_memory_usage: self.plan.estimated_memory_usage,
        }
    }
}

/// Statistics about query processing
#[derive(Debug, Clone)]
pub struct ProcessingStats {
    pub records_processed: usize,
    pub is_windowed: bool,
    pub has_aggregation: bool,
    pub has_joins: bool,
    pub estimated_memory_usage: usize,
}
