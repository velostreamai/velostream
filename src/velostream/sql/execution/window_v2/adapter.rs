//! Adapter Layer for Window V2 Integration
//!
//! This module provides backward-compatible integration between the legacy window
//! processor and the new window_v2 trait-based architecture. It enables gradual
//! migration without breaking existing functionality.
//!
//! ## Architecture
//!
//! The adapter follows the **Adapter Pattern** to bridge two incompatible interfaces:
//!
//! - **Legacy**: `WindowProcessor` with `WindowState` (Vec buffer, simple state)
//! - **New**: `WindowStrategy` traits with Arc<StreamRecord> zero-copy semantics
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Enable window_v2 via feature flag or configuration
//! if context.use_window_v2() {
//!     WindowAdapter::process_with_v2(query_id, query, record, context)?
//! } else {
//!     WindowProcessor::process_windowed_query(query_id, query, record, context)?
//! }
//! ```

use super::emission::{EmitChangesStrategy, EmitFinalStrategy};
use super::strategies::{
    RowsWindowStrategy, SessionWindowStrategy, SlidingWindowStrategy, TumblingWindowStrategy,
};
use super::traits::{EmissionStrategy, WindowStrategy};
use super::types::SharedRecord;
use crate::velostream::sql::SqlError;
use crate::velostream::sql::ast::{
    EmitMode, Expr, RowsEmitMode, SelectField, StreamingQuery, WindowSpec,
};
use crate::velostream::sql::execution::aggregation::accumulator::AccumulatorManager;
use crate::velostream::sql::execution::aggregation::functions::AggregateFunctions;
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::internal::{GroupAccumulator, GroupByState};
use crate::velostream::sql::execution::processors::ProcessorContext;
use crate::velostream::sql::execution::{FieldValue, StreamRecord};
use std::collections::HashMap;

/// Window V2 state stored in ProcessorContext
///
/// This struct wraps the window_v2 strategy and emission strategy,
/// allowing them to be stored and reused across record processing cycles.
pub struct WindowV2State {
    /// The window strategy (Tumbling, Sliding, Session, or Rows)
    pub strategy: Box<dyn WindowStrategy>,
    /// The emission strategy (EmitFinal or EmitChanges)
    pub emission_strategy: Box<dyn EmissionStrategy>,
    /// GROUP BY columns for partitioned processing
    pub group_by_columns: Option<Vec<String>>,
}

/// Adapter for integrating window_v2 with legacy execution engine
pub struct WindowAdapter;

impl WindowAdapter {
    /// Process a windowed query using window_v2 strategies
    ///
    /// This is the main entry point for window_v2 processing, providing
    /// backward-compatible integration with the existing WindowProcessor interface.
    ///
    /// # Arguments
    ///
    /// * `query_id` - Unique identifier for this query
    /// * `query` - The streaming query to execute
    /// * `record` - The incoming stream record
    /// * `context` - Execution context with state management
    ///
    /// # Returns
    ///
    /// - `Ok(Some(StreamRecord))` - Window emitted a result record
    /// - `Ok(None)` - Window buffered the record, no emission yet
    /// - `Err(SqlError)` - Processing error occurred
    pub fn process_with_v2(
        query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<Option<StreamRecord>, SqlError> {
        if let StreamingQuery::Select {
            window,
            where_clause,
            ..
        } = query
        {
            // FR-081 CRITICAL: Apply WHERE clause BEFORE windowing
            // Records that don't match WHERE should not enter window buffers
            if let Some(where_expr) = where_clause {
                let matches = ExpressionEvaluator::evaluate_expression_value(where_expr, record)?;
                match matches {
                    FieldValue::Boolean(false) | FieldValue::Integer(0) => {
                        // Record filtered out by WHERE clause - skip windowing
                        return Ok(None);
                    }
                    _ => {
                        // Record passes WHERE clause - proceed to windowing
                    }
                }
            }

            if let Some(window_spec) = window {
                // Get or create window_v2 state
                let state_key = format!("window_v2:{}", query_id);

                // Check if we need to create new state
                if !Self::has_v2_state(context, &state_key) {
                    Self::initialize_v2_state(context, &state_key, window_spec, query)?;
                }

                // Convert record to SharedRecord for zero-copy processing
                let shared_record = SharedRecord::new(record.clone());

                // Get mutable reference to state (we'll need to work around borrowing issues)
                // For now, we'll use metadata HashMap to store serialized state

                // Process record through window strategy
                Self::process_record_with_strategy(
                    context,
                    &state_key,
                    shared_record,
                    query,
                    window_spec,
                )
            } else {
                Err(SqlError::ExecutionError {
                    message: "No window specification found for windowed query".to_string(),
                    query: None,
                })
            }
        } else {
            Err(SqlError::ExecutionError {
                message: "Invalid query type for WindowAdapter".to_string(),
                query: None,
            })
        }
    }

    /// Check if window_v2 state exists for the given key
    fn has_v2_state(context: &ProcessorContext, state_key: &str) -> bool {
        // FR-081 Phase 2A+: Check window_v2_states HashMap instead of metadata
        // (metadata gets cleared on each context creation, but window_v2_states persists)
        context.window_v2_states.contains_key(state_key)
    }

    /// Initialize window_v2 state for a new query
    fn initialize_v2_state(
        context: &mut ProcessorContext,
        state_key: &str,
        window_spec: &WindowSpec,
        query: &StreamingQuery,
    ) -> Result<(), SqlError> {
        // Create window strategy based on spec
        let strategy = Self::create_strategy(window_spec)?;

        // Create emission strategy based on query
        let emission_strategy = Self::create_emission_strategy(query)?;

        // Extract GROUP BY columns if present
        let group_by_columns = Self::get_group_by_columns(query);

        // Create WindowV2State
        let v2_state = WindowV2State {
            strategy,
            emission_strategy,
            group_by_columns,
        };

        // Store in context's window_v2_states HashMap (boxed as Any for type erasure)
        context
            .window_v2_states
            .insert(state_key.to_string(), Box::new(v2_state));

        // Mark that we have v2 state for this query
        context
            .metadata
            .insert(state_key.to_string(), "initialized".to_string());

        Ok(())
    }

    /// Process a record through the window strategy
    fn process_record_with_strategy(
        context: &mut ProcessorContext,
        state_key: &str,
        record: SharedRecord,
        query: &StreamingQuery,
        _window_spec: &WindowSpec,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Get the window_v2 state (downcast from Any)
        let v2_state_any = context.window_v2_states.get_mut(state_key).ok_or_else(|| {
            SqlError::ExecutionError {
                message: format!("Window V2 state not found for key: {}", state_key),
                query: None,
            }
        })?;

        // Downcast from Box<dyn Any> to WindowV2State
        let v2_state = v2_state_any
            .downcast_mut::<WindowV2State>()
            .ok_or_else(|| SqlError::ExecutionError {
                message: format!("Failed to downcast Window V2 state for key: {}", state_key),
                query: None,
            })?;

        use super::traits::EmitDecision;

        // Process record through emission strategy (which internally uses window strategy)
        let emit_decision = v2_state
            .emission_strategy
            .process_record(record.clone(), &mut *v2_state.strategy)?;

        match emit_decision {
            EmitDecision::Emit | EmitDecision::EmitAndClear => {
                // Get window results from strategy
                let window_records = v2_state.strategy.get_window_records();

                if window_records.is_empty() {
                    return Ok(None);
                }

                // Clear window if requested
                if emit_decision == EmitDecision::EmitAndClear {
                    v2_state.strategy.clear();
                }

                // FR-081 Phase 2A+: Compute aggregations over buffered window records
                // This replaces the naive convert_window_results() approach
                if let StreamingQuery::Select {
                    fields,
                    group_by,
                    having,
                    ..
                } = query
                {
                    // FR-081 Phase 6: Pass HAVING clause to compute_aggregations_over_window
                    // HAVING is evaluated using group accumulators, NOT result field names
                    let computed_results = Self::compute_aggregations_over_window(
                        window_records,
                        fields,
                        group_by,
                        having,
                    )?;

                    if !computed_results.is_empty() {
                        // Queue remaining results for subsequent emissions (if multiple groups)
                        if computed_results.len() > 1 {
                            let query_id =
                                state_key.strip_prefix("window_v2:").unwrap_or(state_key);
                            context.queue_results(query_id, computed_results[1..].to_vec());
                        }

                        // Return first result
                        return Ok(Some(computed_results.into_iter().next().unwrap()));
                    }
                }

                Ok(None)
            }
            EmitDecision::Skip => {
                // No emission this cycle
                Ok(None)
            }
        }
    }

    /// Create a window strategy based on the window specification
    pub fn create_strategy(window_spec: &WindowSpec) -> Result<Box<dyn WindowStrategy>, SqlError> {
        match window_spec {
            WindowSpec::Tumbling { size, time_column } => {
                let window_size_ms = size.as_millis() as i64;
                Ok(Box::new(TumblingWindowStrategy::new(
                    window_size_ms,
                    time_column
                        .clone()
                        .unwrap_or_else(|| "timestamp".to_string()),
                )))
            }
            WindowSpec::Sliding {
                size,
                advance,
                time_column,
            } => {
                let window_size_ms = size.as_millis() as i64;
                let advance_ms = advance.as_millis() as i64;
                Ok(Box::new(SlidingWindowStrategy::new(
                    window_size_ms,
                    advance_ms,
                    time_column
                        .clone()
                        .unwrap_or_else(|| "timestamp".to_string()),
                )))
            }
            WindowSpec::Session {
                gap, time_column, ..
            } => {
                let gap_ms = gap.as_millis() as i64;
                Ok(Box::new(SessionWindowStrategy::new(
                    gap_ms,
                    time_column
                        .clone()
                        .unwrap_or_else(|| "timestamp".to_string()),
                )))
            }
            WindowSpec::Rows {
                buffer_size,
                emit_mode,
                ..
            } => {
                let emit_per_record = matches!(emit_mode, RowsEmitMode::EveryRecord);
                Ok(Box::new(RowsWindowStrategy::new(
                    *buffer_size as usize,
                    emit_per_record,
                )))
            }
        }
    }

    /// Create an emission strategy based on the query
    pub fn create_emission_strategy(
        query: &StreamingQuery,
    ) -> Result<Box<dyn EmissionStrategy>, SqlError> {
        // Check if query has EMIT CHANGES
        let is_emit_changes = Self::is_emit_changes(query);

        if is_emit_changes {
            // EMIT CHANGES: emit on every record (or configurable frequency)
            Ok(Box::new(EmitChangesStrategy::new(1)))
        } else {
            // EMIT FINAL: emit only on window boundaries
            Ok(Box::new(EmitFinalStrategy::new()))
        }
    }

    /// Check if query has EMIT CHANGES clause
    fn is_emit_changes(query: &StreamingQuery) -> bool {
        if let StreamingQuery::Select {
            emit_mode, window, ..
        } = query
        {
            // Check emit_mode field on the SELECT query
            if let Some(mode) = emit_mode {
                matches!(mode, EmitMode::Changes)
            } else {
                // For ROWS windows, check the emit_mode in the window spec
                if let Some(WindowSpec::Rows {
                    emit_mode: rows_emit,
                    ..
                }) = window
                {
                    matches!(rows_emit, RowsEmitMode::EveryRecord)
                } else {
                    false // Default to EMIT FINAL for other windows
                }
            }
        } else {
            false
        }
    }

    /// Extract GROUP BY columns from query
    pub fn get_group_by_columns(query: &StreamingQuery) -> Option<Vec<String>> {
        if let StreamingQuery::Select { group_by, .. } = query {
            if let Some(group_exprs) = group_by {
                // Extract column names from GROUP BY expressions
                let columns: Vec<String> = group_exprs
                    .iter()
                    .filter_map(|expr| {
                        if let crate::velostream::sql::ast::Expr::Column(col) = expr {
                            Some(col.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                if columns.is_empty() {
                    None
                } else {
                    Some(columns)
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Convert window_v2 SharedRecord results back to legacy StreamRecords
    pub fn convert_window_results(
        records: Vec<SharedRecord>,
        _select_fields: &[SelectField],
    ) -> Result<Vec<StreamRecord>, SqlError> {
        records
            .into_iter()
            .map(|shared_rec| {
                // Clone the underlying StreamRecord
                // We need to clone here because the legacy interface expects owned records
                Ok(shared_rec.as_ref().clone())
            })
            .collect()
    }

    /// Compute aggregations over buffered window records (FR-081 Phase 2A+ integration)
    ///
    /// This method integrates window_v2 with the aggregation computation layer,
    /// replacing the naive convert_window_results() approach.
    ///
    /// # Arguments
    ///
    /// * `window_records` - Buffered records from the window strategy
    /// * `fields` - SELECT fields from the query (contains aggregate expressions)
    /// * `group_by` - Optional GROUP BY expressions
    /// * `having` - Optional HAVING clause (evaluated per group using accumulators)
    ///
    /// # Returns
    ///
    /// Vector of computed result records (one per GROUP BY partition that passes HAVING, or single result if no GROUP BY)
    fn compute_aggregations_over_window(
        window_records: Vec<SharedRecord>,
        fields: &[SelectField],
        group_by: &Option<Vec<Expr>>,
        having: &Option<Expr>,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        // Extract aggregate expressions from SELECT fields
        let aggregate_expressions = Self::extract_aggregate_expressions(fields)?;

        // If no aggregations found, return empty (shouldn't happen if we reached here)
        if aggregate_expressions.is_empty() {
            return Ok(Vec::new());
        }

        // FR-081 PERFORMANCE FIX: Use Arc references directly instead of deep cloning
        // Zero-copy semantics: SharedRecord = Arc<StreamRecord>, so we can work with references
        if window_records.is_empty() {
            return Ok(Vec::new());
        }

        // Case 1: No GROUP BY - single accumulator for all records
        if group_by.is_none() || group_by.as_ref().unwrap().is_empty() {
            let mut accumulator = GroupAccumulator::new();

            // Process each record into the accumulator (using Arc references - zero copy!)
            for shared_record in &window_records {
                let record = shared_record.as_ref(); // &StreamRecord (no clone!)
                AccumulatorManager::process_record_into_accumulator(
                    &mut accumulator,
                    record,
                    &aggregate_expressions,
                )?;
            }

            // FR-081 Phase 6: Evaluate HAVING clause for non-GROUP BY queries
            // If HAVING doesn't pass, return empty vec (no result for this window)
            if let Some(having_expr) = having {
                let having_passes =
                    Self::evaluate_having_with_accumulator(having_expr, &accumulator)?;
                if !having_passes {
                    return Ok(Vec::new()); // Window doesn't pass HAVING filter
                }
            }

            // Compute final aggregate values and build result record
            let result_record =
                Self::build_result_record(fields, &aggregate_expressions, &accumulator)?;
            return Ok(vec![result_record]);
        }

        // Case 2: GROUP BY present - create accumulator per partition
        let group_exprs = group_by.as_ref().unwrap();
        let mut group_state = GroupByState::new(group_exprs.clone(), fields.to_vec(), None);

        // Process each record into the appropriate group accumulator (using Arc references - zero copy!)
        for shared_record in &window_records {
            let record = shared_record.as_ref(); // &StreamRecord (no clone!)

            // Evaluate GROUP BY key for this record
            let mut group_key = Vec::new();
            for expr in group_exprs {
                let key_value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
                group_key.push(format!("{:?}", key_value));
            }

            // Get or create accumulator for this group
            let accumulator = group_state.get_or_create_group(group_key);

            // Process record into accumulator
            AccumulatorManager::process_record_into_accumulator(
                accumulator,
                record,
                &aggregate_expressions,
            )?;
        }

        // Build result record for each group (with HAVING filter using accumulators)
        let mut results = Vec::new();
        for (_group_key, accumulator) in &group_state.groups {
            // Evaluate HAVING clause using THIS group's accumulator
            if let Some(having_expr) = having {
                let having_passes =
                    Self::evaluate_having_with_accumulator(having_expr, accumulator)?;
                if !having_passes {
                    // This group doesn't pass HAVING filter - skip it
                    continue;
                }
            }

            // Group passed HAVING (or no HAVING) - build result record
            let result_record =
                Self::build_result_record(fields, &aggregate_expressions, accumulator)?;
            results.push(result_record);
        }

        Ok(results)
    }

    /// Extract aggregate expressions from SELECT fields
    ///
    /// Returns vector of (field_name, expression) pairs for aggregate functions
    fn extract_aggregate_expressions(
        fields: &[SelectField],
    ) -> Result<Vec<(String, Expr)>, SqlError> {
        let mut aggregates = Vec::new();

        for field in fields {
            match field {
                SelectField::Expression { expr, alias } => {
                    // Check if expression is an aggregate function
                    if Self::is_aggregate_expression(expr) {
                        let field_name = alias
                            .clone()
                            .unwrap_or_else(|| format!("{:?}", expr).replace(' ', "_"));
                        aggregates.push((field_name, expr.clone()));
                    }
                }
                SelectField::Column(_) => {
                    // Non-aggregate field - will use sample_record
                }
                SelectField::AliasedColumn { .. } => {
                    // Non-aggregate field - will use sample_record
                }
                SelectField::Wildcard => {
                    // SELECT * - will use sample_record
                }
            }
        }

        Ok(aggregates)
    }

    /// Check if an expression is an aggregate function
    fn is_aggregate_expression(expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                let name_upper = name.to_uppercase();
                matches!(
                    name_upper.as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "STDDEV"
                        | "VARIANCE"
                        | "COUNT_DISTINCT"
                        | "APPROX_COUNT_DISTINCT"
                        | "FIRST"
                        | "LAST"
                        | "STRING_AGG"
                        | "GROUP_CONCAT"
                )
            }
            _ => false,
        }
    }

    /// Build a result StreamRecord from computed aggregate values
    fn build_result_record(
        fields: &[SelectField],
        aggregate_expressions: &[(String, Expr)],
        accumulator: &GroupAccumulator,
    ) -> Result<StreamRecord, SqlError> {
        let mut result_fields = HashMap::new();

        // Process each SELECT field
        for field in fields {
            match field {
                SelectField::Expression { expr, alias } => {
                    if Self::is_aggregate_expression(expr) {
                        // Compute aggregate value
                        let field_name = alias
                            .clone()
                            .unwrap_or_else(|| format!("{:?}", expr).replace(' ', "_"));

                        let aggregate_value = AggregateFunctions::compute_field_aggregate_value(
                            &field_name,
                            expr,
                            accumulator,
                        )?;

                        result_fields.insert(field_name, aggregate_value);
                    } else {
                        // Non-aggregate expression - use sample record
                        if let Some(sample) = &accumulator.sample_record {
                            // Determine field name: use alias, or column name, or formatted expr
                            let field_name = if let Some(alias_name) = alias {
                                alias_name.clone()
                            } else if let Expr::Column(col_name) = expr {
                                // For simple columns, use the column name directly
                                col_name.clone()
                            } else {
                                // For other expressions, format the expr
                                format!("{:?}", expr).replace(' ', "_")
                            };
                            let value =
                                ExpressionEvaluator::evaluate_expression_value(expr, sample)?;
                            result_fields.insert(field_name, value);
                        }
                    }
                }
                SelectField::Column(col_name) => {
                    // Non-aggregate column - use sample record
                    if let Some(sample) = &accumulator.sample_record {
                        if let Some(value) = sample.fields.get(col_name) {
                            result_fields.insert(col_name.clone(), value.clone());
                        }
                    }
                }
                SelectField::AliasedColumn { column, alias } => {
                    // Non-aggregate column with alias - use sample record
                    if let Some(sample) = &accumulator.sample_record {
                        if let Some(value) = sample.fields.get(column) {
                            result_fields.insert(alias.clone(), value.clone());
                        }
                    }
                }
                SelectField::Wildcard => {
                    // SELECT * - include all fields from sample record
                    if let Some(sample) = &accumulator.sample_record {
                        for (key, value) in &sample.fields {
                            result_fields.insert(key.clone(), value.clone());
                        }
                    }
                }
            }
        }

        Ok(StreamRecord::new(result_fields))
    }

    /// Evaluate HAVING clause expression using accumulator for aggregate values.
    ///
    /// # Arguments
    ///
    /// * `having_expr` - HAVING clause expression (may contain aggregate functions)
    /// * `accumulator` - Group accumulator with aggregate values
    ///
    /// # Returns
    ///
    /// Boolean indicating if HAVING condition is satisfied
    fn evaluate_having_with_accumulator(
        having_expr: &Expr,
        accumulator: &GroupAccumulator,
    ) -> Result<bool, SqlError> {
        use crate::velostream::sql::ast::BinaryOperator;

        match having_expr {
            Expr::BinaryOp { left, op, right } => {
                // Evaluate both sides - left may contain aggregate function
                let left_value = if Self::is_aggregate_expression(left) {
                    // For COUNT(*) or COUNT(1), just return the total count
                    if let Expr::Function { name, args } = left.as_ref() {
                        if name.to_uppercase() == "COUNT" {
                            // COUNT(*) or COUNT(1) - return total count
                            if args.is_empty()
                                || (args.len() == 1 && matches!(args[0], Expr::Literal(_)))
                            {
                                FieldValue::Integer(accumulator.count as i64)
                            } else {
                                // COUNT(column) - not supported in HAVING yet
                                FieldValue::Integer(accumulator.count as i64)
                            }
                        } else {
                            // Other aggregates - compute using field
                            AggregateFunctions::compute_field_aggregate_value(
                                "having_agg",
                                left,
                                accumulator,
                            )?
                        }
                    } else {
                        AggregateFunctions::compute_field_aggregate_value(
                            "having_agg",
                            left,
                            accumulator,
                        )?
                    }
                } else {
                    // Non-aggregate expression - should be a literal
                    match left.as_ref() {
                        Expr::Literal(lit) => match lit {
                            crate::velostream::sql::ast::LiteralValue::Integer(i) => {
                                FieldValue::Integer(*i)
                            }
                            crate::velostream::sql::ast::LiteralValue::Float(f) => {
                                FieldValue::Float(*f)
                            }
                            _ => FieldValue::Null,
                        },
                        _ => FieldValue::Null,
                    }
                };

                let right_value = match right.as_ref() {
                    Expr::Literal(lit) => match lit {
                        crate::velostream::sql::ast::LiteralValue::Integer(i) => {
                            FieldValue::Integer(*i)
                        }
                        crate::velostream::sql::ast::LiteralValue::Float(f) => {
                            FieldValue::Float(*f)
                        }
                        _ => FieldValue::Null,
                    },
                    _ => {
                        if Self::is_aggregate_expression(right) {
                            AggregateFunctions::compute_field_aggregate_value(
                                "having_agg",
                                right,
                                accumulator,
                            )?
                        } else {
                            FieldValue::Null
                        }
                    }
                };

                // Apply comparison operator
                Ok(match op {
                    BinaryOperator::GreaterThanOrEqual => match (&left_value, &right_value) {
                        (FieldValue::Integer(l), FieldValue::Integer(r)) => l >= r,
                        (FieldValue::Float(l), FieldValue::Float(r)) => l >= r,
                        (FieldValue::Integer(l), FieldValue::Float(r)) => (*l as f64) >= *r,
                        (FieldValue::Float(l), FieldValue::Integer(r)) => l >= &(*r as f64),
                        _ => false,
                    },
                    BinaryOperator::GreaterThan => match (&left_value, &right_value) {
                        (FieldValue::Integer(l), FieldValue::Integer(r)) => l > r,
                        (FieldValue::Float(l), FieldValue::Float(r)) => l > r,
                        (FieldValue::Integer(l), FieldValue::Float(r)) => (*l as f64) > *r,
                        (FieldValue::Float(l), FieldValue::Integer(r)) => l > &(*r as f64),
                        _ => false,
                    },
                    BinaryOperator::LessThanOrEqual => match (&left_value, &right_value) {
                        (FieldValue::Integer(l), FieldValue::Integer(r)) => l <= r,
                        (FieldValue::Float(l), FieldValue::Float(r)) => l <= r,
                        (FieldValue::Integer(l), FieldValue::Float(r)) => (*l as f64) <= *r,
                        (FieldValue::Float(l), FieldValue::Integer(r)) => l <= &(*r as f64),
                        _ => false,
                    },
                    BinaryOperator::LessThan => match (&left_value, &right_value) {
                        (FieldValue::Integer(l), FieldValue::Integer(r)) => l < r,
                        (FieldValue::Float(l), FieldValue::Float(r)) => l < r,
                        (FieldValue::Integer(l), FieldValue::Float(r)) => (*l as f64) < *r,
                        (FieldValue::Float(l), FieldValue::Integer(r)) => l < &(*r as f64),
                        _ => false,
                    },
                    BinaryOperator::Equal => match (&left_value, &right_value) {
                        (FieldValue::Integer(l), FieldValue::Integer(r)) => l == r,
                        (FieldValue::Float(l), FieldValue::Float(r)) => {
                            (l - r).abs() < f64::EPSILON
                        }
                        (FieldValue::Integer(l), FieldValue::Float(r)) => {
                            ((*l as f64) - r).abs() < f64::EPSILON
                        }
                        (FieldValue::Float(l), FieldValue::Integer(r)) => {
                            (l - (*r as f64)).abs() < f64::EPSILON
                        }
                        _ => false,
                    },
                    BinaryOperator::NotEqual => match (&left_value, &right_value) {
                        (FieldValue::Integer(l), FieldValue::Integer(r)) => l != r,
                        (FieldValue::Float(l), FieldValue::Float(r)) => {
                            (l - r).abs() >= f64::EPSILON
                        }
                        (FieldValue::Integer(l), FieldValue::Float(r)) => {
                            ((*l as f64) - r).abs() >= f64::EPSILON
                        }
                        (FieldValue::Float(l), FieldValue::Integer(r)) => {
                            (l - (*r as f64)).abs() >= f64::EPSILON
                        }
                        _ => false,
                    },
                    _ => false,
                })
            }
            _ => {
                // Unsupported HAVING expression type
                Ok(false)
            }
        }
    }

    /// Evaluate HAVING clause using computed aggregate fields in the result record.
    ///
    /// This method is used for GROUP BY queries where each group has its own computed
    /// aggregate values (COUNT, SUM, AVG, etc.) stored as fields in the result record.
    ///
    /// # Arguments
    ///
    /// * `having_expr` - HAVING clause expression (references aggregate field names)
    /// * `result` - StreamRecord with computed aggregate fields (e.g., "cnt", "total", "avg_amt")
    ///
    /// # Returns
    ///
    /// Boolean indicating if this result passes the HAVING condition
    fn evaluate_having_with_result(
        having_expr: &Expr,
        result: &StreamRecord,
    ) -> Result<bool, SqlError> {
        use crate::velostream::sql::ast::BinaryOperator;

        match having_expr {
            Expr::BinaryOp { left, op, right } => {
                // Evaluate left side - for aggregates, look up the computed field in result
                let left_value = Self::evaluate_having_operand(left, result)?;
                let right_value = Self::evaluate_having_operand(right, result)?;

                // Apply comparison operator
                Ok(match op {
                    BinaryOperator::GreaterThanOrEqual => {
                        Self::compare_values(&left_value, &right_value, |l, r| l >= r)
                    }
                    BinaryOperator::GreaterThan => {
                        Self::compare_values(&left_value, &right_value, |l, r| l > r)
                    }
                    BinaryOperator::LessThanOrEqual => {
                        Self::compare_values(&left_value, &right_value, |l, r| l <= r)
                    }
                    BinaryOperator::LessThan => {
                        Self::compare_values(&left_value, &right_value, |l, r| l < r)
                    }
                    BinaryOperator::Equal => {
                        Self::compare_values(&left_value, &right_value, |l, r| {
                            (l - r).abs() < f64::EPSILON
                        })
                    }
                    BinaryOperator::NotEqual => {
                        Self::compare_values(&left_value, &right_value, |l, r| {
                            (l - r).abs() >= f64::EPSILON
                        })
                    }
                    BinaryOperator::And => {
                        // For AND, both sides should be boolean expressions
                        // Recursively evaluate both sides
                        if let Expr::BinaryOp { .. } = left.as_ref() {
                            let left_result = Self::evaluate_having_with_result(left, result)?;
                            let right_result = Self::evaluate_having_with_result(right, result)?;
                            left_result && right_result
                        } else {
                            false
                        }
                    }
                    _ => false,
                })
            }
            _ => {
                // Unsupported HAVING expression type
                Ok(false)
            }
        }
    }

    /// Evaluate a HAVING operand (left or right side of comparison)
    ///
    /// For aggregate functions like COUNT(*), look up the computed field in the result.
    /// For literals, return the literal value.
    fn evaluate_having_operand(expr: &Expr, result: &StreamRecord) -> Result<FieldValue, SqlError> {
        match expr {
            // Aggregate function - map to computed field name in result
            Expr::Function { name, args } => {
                let func_name = name.to_uppercase();

                // Map aggregate function to its computed field name
                let field_name = if func_name == "COUNT" {
                    // COUNT(*) or COUNT(column) usually aliased as "cnt"
                    if let Some(alias) = Self::find_aggregate_alias_in_result(result, &func_name) {
                        alias
                    } else {
                        "cnt".to_string() // Default COUNT alias
                    }
                } else if func_name == "SUM" {
                    Self::find_aggregate_alias_in_result(result, &func_name)
                        .unwrap_or_else(|| "total".to_string())
                } else if func_name == "AVG" {
                    Self::find_aggregate_alias_in_result(result, &func_name)
                        .unwrap_or_else(|| "avg_amt".to_string())
                } else if func_name == "MAX" {
                    Self::find_aggregate_alias_in_result(result, &func_name)
                        .unwrap_or_else(|| "max_volume".to_string())
                } else {
                    // For other aggregates, try to infer from args
                    func_name.to_lowercase()
                };

                // Look up the computed aggregate value in result fields
                result.fields.get(&field_name).cloned().ok_or_else(|| {
                    SqlError::ExecutionError {
                        message: format!("HAVING: Aggregate field '{}' not found in result. Available fields: {:?}",
                                       field_name, result.fields.keys().collect::<Vec<_>>()),
                        query: None,
                    }
                })
            }
            // Literal value
            Expr::Literal(lit) => match lit {
                crate::velostream::sql::ast::LiteralValue::Integer(i) => {
                    Ok(FieldValue::Integer(*i))
                }
                crate::velostream::sql::ast::LiteralValue::Float(f) => Ok(FieldValue::Float(*f)),
                _ => Ok(FieldValue::Null),
            },
            _ => Ok(FieldValue::Null),
        }
    }

    /// Find the alias for an aggregate function in the result fields
    fn find_aggregate_alias_in_result(result: &StreamRecord, func_name: &str) -> Option<String> {
        // Look for common aggregate aliases
        let func_lower = func_name.to_lowercase();

        // First, try to find a field containing the function name
        for field_name in result.fields.keys() {
            let field_lower = field_name.to_lowercase();
            if field_lower.contains(&func_lower) {
                return Some(field_name.clone());
            }
        }

        // If not found, use heuristics based on field value types
        // For COUNT: Look for any Integer field (excluding dimensions like customer_id, user_id, etc.)
        if func_lower == "count" {
            for (field_name, field_value) in &result.fields {
                if matches!(
                    field_value,
                    crate::velostream::sql::execution::FieldValue::Integer(_)
                ) {
                    let field_lower = field_name.to_lowercase();
                    // Skip common dimension fields
                    if !field_lower.contains("id")
                        && !field_lower.contains("key")
                        && !field_lower.contains("segment")
                    {
                        return Some(field_name.clone());
                    }
                }
            }
        }

        // For SUM/AVG/MAX/MIN: Look for Float or ScaledInteger fields
        if matches!(func_lower.as_str(), "sum" | "avg" | "max" | "min") {
            for (field_name, field_value) in &result.fields {
                if matches!(
                    field_value,
                    crate::velostream::sql::execution::FieldValue::Float(_)
                        | crate::velostream::sql::execution::FieldValue::ScaledInteger(_, _)
                ) {
                    let field_lower = field_name.to_lowercase();
                    // Prefer fields with relevant keywords
                    if func_lower == "sum"
                        && (field_lower.contains("total")
                            || field_lower.contains("sum")
                            || field_lower.contains("value"))
                    {
                        return Some(field_name.clone());
                    }
                    if func_lower == "avg"
                        && (field_lower.contains("avg") || field_lower.contains("average"))
                    {
                        return Some(field_name.clone());
                    }
                    if func_lower == "max" && field_lower.contains("max") {
                        return Some(field_name.clone());
                    }
                    if func_lower == "min" && field_lower.contains("min") {
                        return Some(field_name.clone());
                    }
                }
            }

            // Fallback: return first numeric field
            for (field_name, field_value) in &result.fields {
                if matches!(
                    field_value,
                    crate::velostream::sql::execution::FieldValue::Float(_)
                        | crate::velostream::sql::execution::FieldValue::ScaledInteger(_, _)
                        | crate::velostream::sql::execution::FieldValue::Integer(_)
                ) {
                    let field_lower = field_name.to_lowercase();
                    if !field_lower.contains("id") && !field_lower.contains("key") {
                        return Some(field_name.clone());
                    }
                }
            }
        }

        None
    }

    /// Compare two FieldValues using a comparison function
    fn compare_values<F>(left: &FieldValue, right: &FieldValue, cmp: F) -> bool
    where
        F: Fn(f64, f64) -> bool,
    {
        match (left, right) {
            (FieldValue::Integer(l), FieldValue::Integer(r)) => cmp(*l as f64, *r as f64),
            (FieldValue::Float(l), FieldValue::Float(r)) => cmp(*l, *r),
            (FieldValue::Integer(l), FieldValue::Float(r)) => cmp(*l as f64, *r),
            (FieldValue::Float(l), FieldValue::Integer(r)) => cmp(*l, *r as f64),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_create_tumbling_strategy() {
        let window_spec = WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: Some("event_time".to_string()),
        };

        let strategy = WindowAdapter::create_strategy(&window_spec);
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_create_sliding_strategy() {
        let window_spec = WindowSpec::Sliding {
            size: Duration::from_secs(60),
            advance: Duration::from_secs(30),
            time_column: Some("event_time".to_string()),
        };

        let strategy = WindowAdapter::create_strategy(&window_spec);
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_create_session_strategy() {
        let window_spec = WindowSpec::Session {
            gap: Duration::from_secs(300),
            time_column: Some("event_time".to_string()),
            partition_by: vec![],
        };

        let strategy = WindowAdapter::create_strategy(&window_spec);
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_create_rows_strategy() {
        use crate::velostream::sql::ast::RowExpirationMode;

        let window_spec = WindowSpec::Rows {
            buffer_size: 100,
            partition_by: vec![],
            order_by: vec![],
            time_gap: None,
            window_frame: None,
            emit_mode: RowsEmitMode::EveryRecord,
            expire_after: RowExpirationMode::Never,
        };

        let strategy = WindowAdapter::create_strategy(&window_spec);
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_emit_changes_detection() {
        // Test EMIT CHANGES detection
        let query_emit_changes = StreamingQuery::Select {
            fields: vec![],
            from: crate::velostream::sql::ast::StreamSource::Stream("test".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: Some(WindowSpec::Tumbling {
                size: Duration::from_secs(60),
                time_column: Some("event_time".to_string()),
            }),
            order_by: None,
            limit: None,
            emit_mode: Some(EmitMode::Changes), // EMIT CHANGES
            properties: None,
        };

        assert!(WindowAdapter::is_emit_changes(&query_emit_changes));

        // Test EMIT FINAL (default)
        let query_emit_final = StreamingQuery::Select {
            fields: vec![],
            from: crate::velostream::sql::ast::StreamSource::Stream("test".to_string()),
            from_alias: None,
            joins: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: Some(WindowSpec::Tumbling {
                size: Duration::from_secs(60),
                time_column: Some("event_time".to_string()),
            }),
            order_by: None,
            limit: None,
            emit_mode: Some(EmitMode::Final), // EMIT FINAL
            properties: None,
        };

        assert!(!WindowAdapter::is_emit_changes(&query_emit_final));
    }
}
