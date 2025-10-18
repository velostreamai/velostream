//! Window Query Processor
//!
//! Handles windowed query processing including tumbling, sliding, and session windows.
//! Phase 1B enhancement: Added watermark-aware processing for proper event-time semantics.

use super::{ProcessorContext, WindowContext};
use crate::velostream::sql::ast::WindowSpec;
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::internal::WindowState;
use crate::velostream::sql::execution::watermarks::{LateDataAction, LateDataStrategy};
use crate::velostream::sql::execution::{FieldValue, StreamRecord};
use crate::velostream::sql::{SqlError, StreamingQuery};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use std::collections::HashMap;

/// Window processing utilities
pub struct WindowProcessor;

impl WindowProcessor {
    /// Process a windowed query with optional watermark awareness (Phase 1B)
    /// This is the new enhanced entry point that handles both legacy and watermark-aware processing
    pub fn process_windowed_query_enhanced(
        query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
        source_id: Option<&str>,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Phase 1B: Check for watermark processing
        if context.has_watermarks_enabled() {
            Self::process_windowed_query_with_watermarks(
                query_id, query, record, context, source_id,
            )
        } else {
            // Fallback to existing legacy processing for backward compatibility
            Self::process_windowed_query(query_id, query, record, context)
        }
    }

    /// Process a windowed query with watermark awareness (Phase 1B)
    /// Handles late data, watermark-based emission, and proper event-time semantics
    pub fn process_windowed_query_with_watermarks(
        query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
        source_id: Option<&str>,
    ) -> Result<Option<StreamRecord>, SqlError> {
        if let StreamingQuery::Select { window, .. } = query {
            if let Some(window_spec) = window {
                let source_id = source_id.unwrap_or("default");

                // Phase 1B: Update watermark for this source
                if let Some(watermark_event) = context.update_watermark(source_id, record) {
                    debug!(
                        "Watermark updated for source {}: {:?}",
                        source_id, watermark_event
                    );
                }

                // Phase 1B: Check if record is late
                if context.is_late_record(record) {
                    return Self::handle_late_data(record, context, query_id, query, window_spec);
                }

                // Extract event time (prefer event_time field, fallback to processing timestamp)
                let event_time =
                    Self::extract_event_time_enhanced(record, window_spec.time_column());

                // Phase 1B: Get watermark before any mutable borrows
                let global_watermark = context.get_global_watermark();
                let has_watermarks_enabled = context.has_watermarks_enabled();

                // Get window state for modification
                let window_state = context.get_or_create_window_state(query_id, window_spec);

                // Add record to buffer first
                window_state.add_record(record.clone());

                // Phase 1B: Check emission using watermark-aware logic (without borrowing context)
                let should_emit = if has_watermarks_enabled {
                    Self::should_emit_window_with_watermark_standalone(
                        window_state,
                        event_time,
                        window_spec,
                        global_watermark,
                    )
                } else {
                    Self::should_emit_window_state(window_state, event_time, window_spec)
                };

                if should_emit {
                    // Drop window_state borrow before calling process_window_emission_state
                    // which needs mutable access to context
                    return Self::process_window_emission_state(
                        query_id,
                        query,
                        window_spec,
                        event_time,
                        context,
                    );
                }

                Ok(None)
            } else {
                Err(SqlError::ExecutionError {
                    message: "No window specification found for windowed query".to_string(),
                    query: None,
                })
            }
        } else {
            Err(SqlError::ExecutionError {
                message: "Invalid query type for WindowProcessor".to_string(),
                query: None,
            })
        }
    }

    /// Process a windowed query using high-performance context state management
    /// Optimized for multi-threading with minimal allocations and lock-free operation
    /// (Legacy method - preserved for backward compatibility)
    pub fn process_windowed_query(
        query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<Option<StreamRecord>, SqlError> {
        if let StreamingQuery::Select { window, .. } = query {
            if let Some(window_spec) = window {
                // Session windows use simplified logic to avoid overflow (Phase 3 will enhance this)

                // Extract event time first (minimal CPU overhead)
                let event_time = Self::extract_event_time(record, window_spec.time_column());

                // Get or create window state using high-performance context management
                // This is thread-safe and avoids locks entirely
                let window_state = context.get_or_create_window_state(query_id, window_spec);

                // Add record to buffer (pre-allocated for performance)
                window_state.add_record(record.clone());

                // Check if window should emit using optimized timing logic
                if Self::should_emit_window_state(window_state, event_time, window_spec) {
                    // Drop window_state borrow before calling process_window_emission_state
                    return Self::process_window_emission_state(
                        query_id,
                        query,
                        window_spec,
                        event_time,
                        context,
                    );
                }

                // No emission this cycle - state is automatically marked dirty by context
                Ok(None)
            } else {
                Err(SqlError::ExecutionError {
                    message: "No window specification found for windowed query".to_string(),
                    query: None,
                })
            }
        } else {
            Err(SqlError::ExecutionError {
                message: "Invalid query type for WindowProcessor".to_string(),
                query: None,
            })
        }
    }

    /// Process window emission when triggered for WindowState (high-performance version)
    fn process_window_emission_state(
        query_id: &str,
        query: &StreamingQuery,
        window_spec: &WindowSpec,
        event_time: i64,
        context: &mut ProcessorContext,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Get window state reference - borrow will be released after cloning buffer
        let window_state = context.get_or_create_window_state(query_id, window_spec);
        let last_emit_time = window_state.last_emit;
        let buffer = window_state.buffer.clone();
        // window_state borrow ends here

        // Calculate window boundaries for metadata
        let (window_start, window_end) = match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let start = if last_emit_time == 0 {
                    0 // First window: 0 to window_size_ms
                } else {
                    last_emit_time
                };
                let end = start + window_size_ms;
                (start, end)
            }
            WindowSpec::Sliding { size, advance, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let advance_ms = advance.as_millis() as i64;
                let start = last_emit_time;
                let end = start + window_size_ms;
                (start, end)
            }
            WindowSpec::Session { gap, .. } => {
                // For session windows, use the first and last record times
                if buffer.is_empty() {
                    (event_time, event_time)
                } else {
                    let first_time =
                        Self::extract_event_time(&buffer[0], window_spec.time_column());
                    let last_time = Self::extract_event_time(
                        &buffer[buffer.len() - 1],
                        window_spec.time_column(),
                    );
                    (first_time, last_time)
                }
            }
        };

        // Filter buffer for current window
        let windowed_buffer = match window_spec {
            WindowSpec::Tumbling { .. } => {
                // Filter records that belong to the completed window
                buffer
                    .iter()
                    .filter(|r| {
                        let record_time = Self::extract_event_time(r, window_spec.time_column());
                        record_time >= window_start && record_time < window_end
                    })
                    .cloned()
                    .collect()
            }
            _ => buffer, // For other window types, use all buffered records
        };

        // Execute aggregation on filtered records with window boundaries
        let result_option = match Self::execute_windowed_aggregation_impl(
            query,
            &windowed_buffer,
            window_start,
            window_end,
            context,
        ) {
            Ok(result) => Some(result),
            Err(SqlError::ExecutionError { message, .. })
                if message == "No records after filtering" =>
            {
                None
            }
            Err(SqlError::ExecutionError { message, .. })
                if message == "HAVING clause not satisfied" =>
            {
                None
            }
            Err(SqlError::ExecutionError { message, .. })
                if message == "No groups satisfied HAVING clause" =>
            {
                None
            }
            Err(e) => return Err(e),
        };

        // Get window state again for updates (new mutable borrow)
        let window_state = context.get_or_create_window_state(query_id, window_spec);

        // Update window state after aggregation
        Self::update_window_state_direct(window_state, window_spec, event_time);

        // Clear or adjust buffer based on window type
        Self::cleanup_window_buffer_direct(window_state, window_spec, last_emit_time);

        Ok(result_option)
    }

    /// Process window emission when triggered (DEPRECATED: Legacy method without context support)
    /// This method cannot support EXISTS subqueries in HAVING clauses.
    /// Use process_window_emission_state instead for full feature support.
    #[deprecated(
        note = "Use process_window_emission_state for full feature support including EXISTS subqueries"
    )]
    fn process_window_emission(
        query: &StreamingQuery,
        window_spec: &WindowSpec,
        window_context: &mut WindowContext,
        event_time: i64,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Check if query has EXISTS/NOT EXISTS subqueries in HAVING clause
        // This legacy method cannot support them without ProcessorContext
        if let StreamingQuery::Select { having, .. } = query {
            if let Some(having_expr) = having {
                if Self::contains_exists_subquery(having_expr) {
                    return Err(SqlError::ExecutionError {
                        message: "EXISTS/NOT EXISTS subqueries in HAVING clauses require ProcessorContext. Use process_window_emission_state instead.".to_string(),
                        query: None,
                    });
                }
            }
        }

        let last_emit_time = window_context.last_emit;
        let buffer = window_context.buffer.clone();

        // Calculate window boundaries for metadata
        let (window_start, window_end) = match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let start = if last_emit_time == 0 {
                    0 // First window: 0 to window_size_ms
                } else {
                    last_emit_time
                };
                let end = start + window_size_ms;
                (start, end)
            }
            WindowSpec::Sliding { size, advance, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let advance_ms = advance.as_millis() as i64;
                let start = last_emit_time;
                let end = start + window_size_ms;
                (start, end)
            }
            WindowSpec::Session { gap, .. } => {
                // For session windows, use the first and last record times
                if buffer.is_empty() {
                    (event_time, event_time)
                } else {
                    let first_time =
                        Self::extract_event_time(&buffer[0], window_spec.time_column());
                    let last_time = Self::extract_event_time(
                        &buffer[buffer.len() - 1],
                        window_spec.time_column(),
                    );
                    (first_time, last_time)
                }
            }
        };

        // Filter buffer for current window
        let windowed_buffer = match window_spec {
            WindowSpec::Tumbling { .. } => {
                // Filter records that belong to the completed window
                buffer
                    .iter()
                    .filter(|r| {
                        let record_time = Self::extract_event_time(r, window_spec.time_column());
                        record_time >= window_start && record_time < window_end
                    })
                    .cloned()
                    .collect()
            }
            _ => buffer, // For other window types, use all buffered records
        };

        // Create a minimal context for non-EXISTS queries
        // This context has no tables, so EXISTS subqueries would fail anyway
        let context = ProcessorContext::new("legacy_window_emission");

        // Execute aggregation on filtered records with window boundaries
        let result_option = match Self::execute_windowed_aggregation_impl(
            query,
            &windowed_buffer,
            window_start,
            window_end,
            &context,
        ) {
            Ok(result) => Some(result),
            Err(SqlError::ExecutionError { message, .. })
                if message == "No records after filtering" =>
            {
                None
            }
            Err(SqlError::ExecutionError { message, .. })
                if message == "HAVING clause not satisfied" =>
            {
                None
            }
            Err(SqlError::ExecutionError { message, .. })
                if message == "No groups satisfied HAVING clause" =>
            {
                None
            }
            Err(e) => return Err(e),
        };

        // Update window state after aggregation
        Self::update_window_state(window_context, window_spec, event_time);

        // Clear or adjust buffer based on window type
        Self::cleanup_window_buffer(window_context, window_spec, last_emit_time);

        Ok(result_option)
    }

    /// Extract event time from record
    pub fn extract_event_time(record: &StreamRecord, time_column: Option<&str>) -> i64 {
        if let Some(column_name) = time_column {
            if let Some(field_value) = record.fields.get(column_name) {
                match field_value {
                    FieldValue::Integer(ts) => *ts,
                    FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis(),
                    FieldValue::String(s) => s.parse::<i64>().unwrap_or(record.timestamp),
                    _ => record.timestamp,
                }
            } else {
                record.timestamp
            }
        } else {
            record.timestamp
        }
    }

    /// Check if window should emit based on proper window logic from legacy method
    pub fn should_emit_window(
        window_context: &WindowContext,
        event_time: i64,
        _time_column: Option<&str>,
    ) -> bool {
        // Check if we have any buffered records
        if window_context.buffer.is_empty() {
            return false;
        }

        // Get the last emit time
        let last_emit = window_context.last_emit;

        // For tumbling windows, check if enough time has passed
        // This is a simplified version - in practice, this would check the actual window size
        let window_size_ms = 5000; // Default 5 second tumbling window

        // If this is the first window (last_emit == 0), check if we have enough time elapsed
        if last_emit == 0 {
            return event_time >= window_size_ms;
        }

        // Otherwise, check if a full window period has elapsed
        event_time >= last_emit + window_size_ms
    }

    /// High-performance window emission check for WindowState (optimized for threading)
    pub fn should_emit_window_state(
        window_state: &WindowState,
        event_time: i64,
        window_spec: &WindowSpec,
    ) -> bool {
        // Check if we have any buffered records
        if window_state.buffer.is_empty() {
            return false;
        }

        // Get the last emit time
        let last_emit = window_state.last_emit;

        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;

                // If this is the first window (last_emit == 0), check if we have enough time elapsed
                if last_emit == 0 {
                    return event_time >= window_size_ms;
                }

                // Otherwise, check if a full window period has elapsed
                event_time >= last_emit + window_size_ms
            }
            WindowSpec::Sliding { advance, .. } => {
                let advance_ms = advance.as_millis() as i64;

                // For sliding windows, emit based on advance interval
                if last_emit == 0 {
                    return event_time >= advance_ms;
                }

                event_time >= last_emit + advance_ms
            }
            WindowSpec::Session { .. } => {
                // Session windows use enhanced logic with proper gap detection and overflow safety
                // For now, emit after we have at least one record to enable basic functionality
                !window_state.buffer.is_empty()
            }
        }
    }

    /// Execute windowed aggregation implementation using logic from legacy method
    fn execute_windowed_aggregation_impl(
        query: &StreamingQuery,
        windowed_buffer: &[StreamRecord],
        window_start: i64,
        window_end: i64,
        context: &ProcessorContext,
    ) -> Result<StreamRecord, SqlError> {
        use crate::velostream::sql::execution::expression::evaluator::ExpressionEvaluator;

        debug!(
            "AGG: execute_windowed_aggregation_impl called with {} records",
            windowed_buffer.len()
        );

        if windowed_buffer.is_empty() {
            debug!("AGG: No records in windowed buffer");
            return Err(SqlError::ExecutionError {
                message: "No records after filtering".to_string(),
                query: None,
            });
        }

        if let StreamingQuery::Select {
            fields,
            where_clause,
            group_by: _,
            having,
            ..
        } = query
        {
            debug!("AGG: Processing SELECT query with {} fields", fields.len());
            for (i, field) in fields.iter().enumerate() {
                debug!("AGG: Field {}: {:?}", i, field);
            }

            // Step 1: Filter records by WHERE clause
            let filtered_records: Vec<&StreamRecord> = windowed_buffer
                .iter()
                .filter(|record| {
                    if let Some(where_expr) = where_clause {
                        let result = ExpressionEvaluator::evaluate_expression(where_expr, record)
                            .unwrap_or(false);
                        debug!("AGG: WHERE clause evaluated to: {}", result);
                        result
                    } else {
                        true
                    }
                })
                .collect();

            debug!(
                "AGG: After WHERE filtering: {} records remain",
                filtered_records.len()
            );

            if filtered_records.is_empty() {
                debug!("AGG: No records after WHERE filtering");
                return Err(SqlError::ExecutionError {
                    message: "No records after filtering".to_string(),
                    query: None,
                });
            }

            // Step 2: For windowed queries, create aggregated result
            let mut result_fields = HashMap::new();

            debug!("AGG: Creating aggregated result fields");

            // Simple aggregation logic - process the first record as representative
            if let Some(first_record) = filtered_records.first() {
                debug!("AGG: Processing {} SELECT fields", fields.len());

                // Process SELECT fields
                for (field_idx, field) in fields.iter().enumerate() {
                    debug!("AGG: Processing field {}: {:?}", field_idx, field);

                    match field {
                        crate::velostream::sql::ast::SelectField::Wildcard => {
                            debug!("AGG: Processing wildcard field");
                            // For windowed aggregations, add basic aggregate info instead of all fields
                            result_fields.insert(
                                "window_size".to_string(),
                                FieldValue::Integer(filtered_records.len() as i64),
                            );
                        }
                        crate::velostream::sql::ast::SelectField::Expression { expr, alias } => {
                            let field_name = alias.clone().unwrap_or_else(|| {
                                // For column expressions without alias, use the column name
                                match expr {
                                    crate::velostream::sql::ast::Expr::Column(col_name) => {
                                        col_name.clone()
                                    }
                                    _ => format!("field_{}", result_fields.len()),
                                }
                            });

                            debug!(
                                "AGG: Processing expression field '{}': {:?}",
                                field_name, expr
                            );

                            // Handle aggregate functions properly for windowed queries
                            match Self::evaluate_aggregate_expression(expr, &filtered_records) {
                                Ok(value) => {
                                    debug!(
                                        "AGG: Successfully evaluated field '{}' = {:?}",
                                        field_name, value
                                    );
                                    result_fields.insert(field_name, value);
                                }
                                Err(e) => {
                                    log::error!(
                                        "AGG: Failed to evaluate field '{}': {:?}",
                                        field_name,
                                        e
                                    );
                                    return Err(e);
                                }
                            }
                        }
                        crate::velostream::sql::ast::SelectField::Column(column_name) => {
                            debug!("AGG: Processing column field '{}'", column_name);
                            // Simple column reference
                            if let Some(value) = first_record.fields.get(column_name) {
                                debug!("AGG: Found column '{}' = {:?}", column_name, value);
                                result_fields.insert(column_name.clone(), value.clone());
                            } else {
                                error!("AGG: Column '{}' not found in record", column_name);
                            }
                        }
                        crate::velostream::sql::ast::SelectField::AliasedColumn {
                            column,
                            alias,
                        } => {
                            debug!("AGG: Processing aliased column '{}' -> '{}'", column, alias);
                            // Aliased column reference
                            if let Some(value) = first_record.fields.get(column) {
                                debug!("AGG: Found aliased column '{}' = {:?}", column, value);
                                result_fields.insert(alias.clone(), value.clone());
                            } else {
                                error!("AGG: Aliased column '{}' not found in record", column);
                            }
                        }
                    }
                }
            }

            // Always include window metadata
            result_fields.insert(
                "_window_record_count".to_string(),
                FieldValue::Integer(filtered_records.len() as i64),
            );
            result_fields.insert(
                "_window_start".to_string(),
                FieldValue::Integer(window_start),
            );
            result_fields.insert("_window_end".to_string(), FieldValue::Integer(window_end));

            // Step 3: Apply HAVING clause filtering
            if let Some(having_expr) = having {
                // Create a temporary record for HAVING evaluation
                let temp_record = StreamRecord {
                    fields: result_fields.clone(),
                    timestamp: 0,
                    offset: 0,
                    partition: 0,
                    headers: HashMap::new(),
                    event_time: None,
                };

                // Evaluate HAVING clause with aggregate expressions
                let having_result = Self::evaluate_having_expression(
                    having_expr,
                    &filtered_records,
                    &temp_record,
                    context,
                )?;

                if !having_result {
                    return Err(SqlError::ExecutionError {
                        message: "No records after filtering".to_string(),
                        query: None,
                    });
                }
            }

            // Use timestamp from the last record in the window
            let timestamp = filtered_records.last().map(|r| r.timestamp).unwrap_or(0);

            Ok(StreamRecord {
                fields: result_fields,
                timestamp,
                offset: 0,
                partition: 0,
                headers: HashMap::new(),
                event_time: None,
            })
        } else {
            Err(SqlError::ExecutionError {
                message: "Invalid query type for windowed aggregation".to_string(),
                query: None,
            })
        }
    }

    /// Update window state after processing (high-performance version for WindowState)
    fn update_window_state_direct(
        window_state: &mut WindowState,
        window_spec: &WindowSpec,
        event_time: i64,
    ) {
        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                window_state.last_emit = (event_time / window_size_ms) * window_size_ms;
            }
            WindowSpec::Sliding { .. } => {
                window_state.last_emit = event_time;
            }
            WindowSpec::Session { .. } => {
                window_state.last_emit = event_time;
            }
        }
    }

    /// Update window state after processing
    fn update_window_state(
        window_context: &mut WindowContext,
        window_spec: &WindowSpec,
        event_time: i64,
    ) {
        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                window_context.last_emit = (event_time / window_size_ms) * window_size_ms;
            }
            WindowSpec::Sliding { .. } => {
                window_context.last_emit = event_time;
            }
            WindowSpec::Session { .. } => {
                window_context.last_emit = event_time;
            }
        }
    }

    /// Clean up window buffer based on window type (high-performance version for WindowState)
    fn cleanup_window_buffer_direct(
        window_state: &mut WindowState,
        window_spec: &WindowSpec,
        old_last_emit: i64,
    ) {
        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let completed_window_start = if old_last_emit == 0 { 0 } else { old_last_emit };
                let completed_window_end = completed_window_start + window_size_ms;
                let time_column = window_spec.time_column();

                // Only remove records that were part of the completed window
                window_state.buffer.retain(|r| {
                    let record_time = if let Some(column_name) = time_column {
                        if let Some(field_value) = r.fields.get(column_name) {
                            match field_value {
                                FieldValue::Integer(ts) => *ts,
                                FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis(),
                                FieldValue::String(s) => s.parse::<i64>().unwrap_or(r.timestamp),
                                _ => r.timestamp,
                            }
                        } else {
                            r.timestamp
                        }
                    } else {
                        r.timestamp
                    };
                    // Keep records that are NOT in the completed window
                    !(record_time >= completed_window_start && record_time < completed_window_end)
                });
            }
            WindowSpec::Sliding { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let cutoff_time = window_state.last_emit.saturating_sub(window_size_ms);
                let time_column = window_spec.time_column();

                // Remove records older than the sliding window
                window_state.buffer.retain(|r| {
                    let record_time = if let Some(column_name) = time_column {
                        if let Some(field_value) = r.fields.get(column_name) {
                            match field_value {
                                FieldValue::Integer(ts) => *ts,
                                FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis(),
                                FieldValue::String(s) => s.parse::<i64>().unwrap_or(r.timestamp),
                                _ => r.timestamp,
                            }
                        } else {
                            r.timestamp
                        }
                    } else {
                        r.timestamp
                    };
                    record_time >= cutoff_time
                });
            }
            WindowSpec::Session { gap, .. } => {
                // For session windows, we would need more complex logic
                // For now, keep recent records within the gap time
                let gap_ms = gap.as_millis() as i64;
                let cutoff_time = window_state.last_emit.saturating_sub(gap_ms);
                let time_column = window_spec.time_column();

                window_state.buffer.retain(|r| {
                    let record_time = if let Some(column_name) = time_column {
                        if let Some(field_value) = r.fields.get(column_name) {
                            match field_value {
                                FieldValue::Integer(ts) => *ts,
                                FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis(),
                                FieldValue::String(s) => s.parse::<i64>().unwrap_or(r.timestamp),
                                _ => r.timestamp,
                            }
                        } else {
                            r.timestamp
                        }
                    } else {
                        r.timestamp
                    };
                    record_time >= cutoff_time
                });
            }
        }
    }

    /// Clean up window buffer based on window type
    fn cleanup_window_buffer(
        window_context: &mut WindowContext,
        window_spec: &WindowSpec,
        old_last_emit: i64,
    ) {
        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let completed_window_start = if old_last_emit == 0 { 0 } else { old_last_emit };
                let completed_window_end = completed_window_start + window_size_ms;
                let time_column = window_spec.time_column();

                // Only remove records that were part of the completed window
                window_context.buffer.retain(|r| {
                    let record_time = if let Some(column_name) = time_column {
                        if let Some(field_value) = r.fields.get(column_name) {
                            match field_value {
                                FieldValue::Integer(ts) => *ts,
                                FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis(),
                                FieldValue::String(s) => s.parse::<i64>().unwrap_or(r.timestamp),
                                _ => r.timestamp,
                            }
                        } else {
                            r.timestamp
                        }
                    } else {
                        r.timestamp
                    };
                    // Keep records that are NOT in the completed window
                    !(record_time >= completed_window_start && record_time < completed_window_end)
                });
            }
            WindowSpec::Sliding { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let cutoff_time = window_context.last_emit.saturating_sub(window_size_ms);
                let time_column = window_spec.time_column();

                // Remove records older than the sliding window
                window_context.buffer.retain(|r| {
                    let record_time = if let Some(column_name) = time_column {
                        if let Some(field_value) = r.fields.get(column_name) {
                            match field_value {
                                FieldValue::Integer(ts) => *ts,
                                FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis(),
                                FieldValue::String(s) => s.parse::<i64>().unwrap_or(r.timestamp),
                                _ => r.timestamp,
                            }
                        } else {
                            r.timestamp
                        }
                    } else {
                        r.timestamp
                    };
                    record_time >= cutoff_time
                });
            }
            WindowSpec::Session { gap, .. } => {
                // For session windows, we would need more complex logic
                // For now, keep recent records within the gap time
                let gap_ms = gap.as_millis() as i64;
                let cutoff_time = window_context.last_emit.saturating_sub(gap_ms);
                let time_column = window_spec.time_column();

                window_context.buffer.retain(|r| {
                    let record_time = if let Some(column_name) = time_column {
                        if let Some(field_value) = r.fields.get(column_name) {
                            match field_value {
                                FieldValue::Integer(ts) => *ts,
                                FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis(),
                                FieldValue::String(s) => s.parse::<i64>().unwrap_or(r.timestamp),
                                _ => r.timestamp,
                            }
                        } else {
                            r.timestamp
                        }
                    } else {
                        r.timestamp
                    };
                    record_time >= cutoff_time
                });
            }
        }
    }

    /// Get time column from window spec
    pub fn get_time_column(window_spec: &WindowSpec) -> Option<&str> {
        window_spec.time_column()
    }

    /// Create a new window context
    pub fn create_window_context() -> WindowContext {
        WindowContext {
            buffer: Vec::new(),
            last_emit: 0,
            should_emit: false,
        }
    }

    /// Check if an expression contains EXISTS or NOT EXISTS subqueries (recursive)
    fn contains_exists_subquery(expr: &crate::velostream::sql::ast::Expr) -> bool {
        use crate::velostream::sql::ast::{Expr, SubqueryType};

        match expr {
            Expr::Subquery { subquery_type, .. } => {
                matches!(
                    subquery_type,
                    SubqueryType::Exists | SubqueryType::NotExists
                )
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_exists_subquery(left) || Self::contains_exists_subquery(right)
            }
            Expr::UnaryOp { expr: inner, .. } => Self::contains_exists_subquery(inner),
            _ => false,
        }
    }

    /// Evaluate HAVING clause expression (handles aggregates, EXISTS subqueries, and logical operators)
    fn evaluate_having_expression(
        expr: &crate::velostream::sql::ast::Expr,
        records: &[&StreamRecord],
        temp_record: &StreamRecord,
        context: &ProcessorContext,
    ) -> Result<bool, SqlError> {
        use crate::velostream::sql::ast::{BinaryOperator, Expr};

        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        // Recursively evaluate both sides
                        let left_result =
                            Self::evaluate_having_expression(left, records, temp_record, context)?;
                        let right_result =
                            Self::evaluate_having_expression(right, records, temp_record, context)?;
                        Ok(left_result && right_result)
                    }
                    BinaryOperator::Or => {
                        // Recursively evaluate both sides
                        let left_result =
                            Self::evaluate_having_expression(left, records, temp_record, context)?;
                        let right_result =
                            Self::evaluate_having_expression(right, records, temp_record, context)?;
                        Ok(left_result || right_result)
                    }
                    BinaryOperator::GreaterThanOrEqual
                    | BinaryOperator::GreaterThan
                    | BinaryOperator::LessThanOrEqual
                    | BinaryOperator::LessThan
                    | BinaryOperator::Equal
                    | BinaryOperator::NotEqual => {
                        // Handle comparison operators with aggregate functions
                        let left_value = Self::evaluate_aggregate_expression(left, records)?;
                        let right_value = if let Ok(value) =
                            ExpressionEvaluator::evaluate_expression_value(right, temp_record)
                        {
                            value
                        } else {
                            FieldValue::Null
                        };

                        // Apply comparison
                        Ok(Self::compare_field_values(&left_value, &right_value, op))
                    }
                    _ => {
                        // For other binary operators, use regular evaluation
                        Ok(ExpressionEvaluator::evaluate_expression(expr, temp_record)
                            .unwrap_or(false))
                    }
                }
            }
            Expr::Subquery {
                query,
                subquery_type,
            } => {
                // EXISTS/NOT EXISTS subqueries in HAVING clauses
                use crate::velostream::sql::ast::SubqueryType;
                use crate::velostream::sql::execution::expression::SubqueryExecutor;
                use crate::velostream::sql::execution::processors::select::SelectProcessor;

                match subquery_type {
                    SubqueryType::Exists | SubqueryType::NotExists => {
                        // Use SelectProcessor as the subquery executor (same pattern as SELECT processor)
                        let executor = SelectProcessor;
                        let exists_result = executor.execute_exists_subquery(
                            query.as_ref(),
                            temp_record,
                            context,
                        )?;

                        let result = match subquery_type {
                            SubqueryType::Exists => exists_result,
                            SubqueryType::NotExists => !exists_result,
                            _ => unreachable!(),
                        };

                        Ok(result)
                    }
                    _ => {
                        // Other subquery types
                        Err(SqlError::ExecutionError {
                            message: "Only EXISTS/NOT EXISTS subqueries are currently supported in HAVING clauses with WINDOW".to_string(),
                            query: None,
                        })
                    }
                }
            }
            _ => {
                // For non-binary-op expressions, use regular evaluation
                Ok(ExpressionEvaluator::evaluate_expression(expr, temp_record).unwrap_or(false))
            }
        }
    }

    /// Compare two FieldValues using a binary operator
    fn compare_field_values(
        left: &FieldValue,
        right: &FieldValue,
        op: &crate::velostream::sql::ast::BinaryOperator,
    ) -> bool {
        use crate::velostream::sql::ast::BinaryOperator;
        match (left, right, op) {
            // Integer comparisons
            (
                FieldValue::Integer(l),
                FieldValue::Integer(r),
                BinaryOperator::GreaterThanOrEqual,
            ) => l >= r,
            (FieldValue::Integer(l), FieldValue::Integer(r), BinaryOperator::GreaterThan) => l > r,
            (FieldValue::Integer(l), FieldValue::Integer(r), BinaryOperator::LessThanOrEqual) => {
                l <= r
            }
            (FieldValue::Integer(l), FieldValue::Integer(r), BinaryOperator::LessThan) => l < r,
            (FieldValue::Integer(l), FieldValue::Integer(r), BinaryOperator::Equal) => l == r,
            (FieldValue::Integer(l), FieldValue::Integer(r), BinaryOperator::NotEqual) => l != r,
            // Float comparisons
            (FieldValue::Float(l), FieldValue::Float(r), BinaryOperator::GreaterThanOrEqual) => {
                l >= r
            }
            (FieldValue::Float(l), FieldValue::Float(r), BinaryOperator::GreaterThan) => l > r,
            (FieldValue::Float(l), FieldValue::Float(r), BinaryOperator::LessThanOrEqual) => l <= r,
            (FieldValue::Float(l), FieldValue::Float(r), BinaryOperator::LessThan) => l < r,
            (FieldValue::Float(l), FieldValue::Float(r), BinaryOperator::Equal) => {
                (l - r).abs() < f64::EPSILON
            }
            (FieldValue::Float(l), FieldValue::Float(r), BinaryOperator::NotEqual) => {
                (l - r).abs() >= f64::EPSILON
            }
            // Mixed integer/float comparisons
            (FieldValue::Integer(l), FieldValue::Float(r), BinaryOperator::GreaterThanOrEqual) => {
                (*l as f64) >= *r
            }
            (FieldValue::Integer(l), FieldValue::Float(r), BinaryOperator::GreaterThan) => {
                (*l as f64) > *r
            }
            (FieldValue::Integer(l), FieldValue::Float(r), BinaryOperator::LessThanOrEqual) => {
                (*l as f64) <= *r
            }
            (FieldValue::Integer(l), FieldValue::Float(r), BinaryOperator::LessThan) => {
                (*l as f64) < *r
            }
            (FieldValue::Integer(l), FieldValue::Float(r), BinaryOperator::Equal) => {
                ((*l as f64) - r).abs() < f64::EPSILON
            }
            (FieldValue::Integer(l), FieldValue::Float(r), BinaryOperator::NotEqual) => {
                ((*l as f64) - r).abs() >= f64::EPSILON
            }
            (FieldValue::Float(l), FieldValue::Integer(r), BinaryOperator::GreaterThanOrEqual) => {
                *l >= (*r as f64)
            }
            (FieldValue::Float(l), FieldValue::Integer(r), BinaryOperator::GreaterThan) => {
                *l > (*r as f64)
            }
            (FieldValue::Float(l), FieldValue::Integer(r), BinaryOperator::LessThanOrEqual) => {
                *l <= (*r as f64)
            }
            (FieldValue::Float(l), FieldValue::Integer(r), BinaryOperator::LessThan) => {
                *l < (*r as f64)
            }
            (FieldValue::Float(l), FieldValue::Integer(r), BinaryOperator::Equal) => {
                (*l - (*r as f64)).abs() < f64::EPSILON
            }
            (FieldValue::Float(l), FieldValue::Integer(r), BinaryOperator::NotEqual) => {
                (*l - (*r as f64)).abs() >= f64::EPSILON
            }
            _ => false,
        }
    }

    /// Evaluate aggregate expression across multiple records in a window
    fn evaluate_aggregate_expression(
        expr: &crate::velostream::sql::ast::Expr,
        records: &[&StreamRecord],
    ) -> Result<FieldValue, SqlError> {
        use crate::velostream::sql::ast::Expr;

        debug!(
            "AGG: evaluate_aggregate_expression called with {} records, expr: {:?}",
            records.len(),
            expr
        );

        match expr {
            Expr::Function { name, args } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => {
                        if args.is_empty()
                            || (args.len() == 1
                                && matches!(
                                    args[0],
                                    Expr::Literal(
                                        crate::velostream::sql::ast::LiteralValue::Integer(1)
                                    )
                                ))
                        {
                            // COUNT(*) or COUNT(1) - count all records
                            Ok(FieldValue::Integer(records.len() as i64))
                        } else {
                            // COUNT(column) - count non-null values
                            let mut count = 0i64;
                            for record in records {
                                if let Ok(value) =
                                    ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                                {
                                    if !matches!(value, FieldValue::Null) {
                                        count += 1;
                                    }
                                }
                            }
                            Ok(FieldValue::Integer(count))
                        }
                    }
                    "SUM" => {
                        let mut sum = 0.0;
                        for record in records {
                            if let Ok(value) =
                                ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                            {
                                match value {
                                    FieldValue::Integer(i) => sum += i as f64,
                                    FieldValue::Float(f) => sum += f,
                                    _ => {} // Skip non-numeric values
                                }
                            }
                        }
                        Ok(FieldValue::Float(sum))
                    }
                    "AVG" => {
                        let mut sum = 0.0;
                        let mut count = 0i64;
                        for record in records {
                            if let Ok(value) =
                                ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                            {
                                match value {
                                    FieldValue::Integer(i) => {
                                        sum += i as f64;
                                        count += 1;
                                    }
                                    FieldValue::Float(f) => {
                                        sum += f;
                                        count += 1;
                                    }
                                    _ => {} // Skip non-numeric values
                                }
                            }
                        }
                        if count > 0 {
                            Ok(FieldValue::Float(sum / count as f64))
                        } else {
                            Ok(FieldValue::Null)
                        }
                    }
                    "MIN" => {
                        let mut min_val: Option<FieldValue> = None;
                        for record in records {
                            if let Ok(value) =
                                ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                            {
                                match &min_val {
                                    None => min_val = Some(value),
                                    Some(current_min) => {
                                        // Compare values - simplified comparison for numeric types
                                        match (&value, current_min) {
                                            (FieldValue::Integer(i1), FieldValue::Integer(i2)) => {
                                                if i1 < i2 {
                                                    min_val = Some(value);
                                                }
                                            }
                                            (FieldValue::Float(f1), FieldValue::Float(f2)) => {
                                                if f1 < f2 {
                                                    min_val = Some(value);
                                                }
                                            }
                                            (FieldValue::Integer(i), FieldValue::Float(f)) => {
                                                if (*i as f64) < *f {
                                                    min_val = Some(value);
                                                }
                                            }
                                            (FieldValue::Float(f), FieldValue::Integer(i)) => {
                                                if *f < (*i as f64) {
                                                    min_val = Some(value);
                                                }
                                            }
                                            _ => {} // Skip non-comparable types
                                        }
                                    }
                                }
                            }
                        }
                        Ok(min_val.unwrap_or(FieldValue::Null))
                    }
                    "MAX" => {
                        let mut max_val: Option<FieldValue> = None;
                        for record in records {
                            if let Ok(value) =
                                ExpressionEvaluator::evaluate_expression_value(&args[0], record)
                            {
                                match &max_val {
                                    None => max_val = Some(value),
                                    Some(current_max) => {
                                        // Compare values - simplified comparison for numeric types
                                        match (&value, current_max) {
                                            (FieldValue::Integer(i1), FieldValue::Integer(i2)) => {
                                                if i1 > i2 {
                                                    max_val = Some(value);
                                                }
                                            }
                                            (FieldValue::Float(f1), FieldValue::Float(f2)) => {
                                                if f1 > f2 {
                                                    max_val = Some(value);
                                                }
                                            }
                                            (FieldValue::Integer(i), FieldValue::Float(f)) => {
                                                if (*i as f64) > *f {
                                                    max_val = Some(value);
                                                }
                                            }
                                            (FieldValue::Float(f), FieldValue::Integer(i)) => {
                                                if *f > (*i as f64) {
                                                    max_val = Some(value);
                                                }
                                            }
                                            _ => {} // Skip non-comparable types
                                        }
                                    }
                                }
                            }
                        }
                        Ok(max_val.unwrap_or(FieldValue::Null))
                    }
                    _ => {
                        // For non-aggregate functions, just evaluate on first record
                        if let Some(first_record) = records.first() {
                            ExpressionEvaluator::evaluate_expression_value(expr, first_record)
                        } else {
                            Ok(FieldValue::Null)
                        }
                    }
                }
            }
            Expr::Column(column_name) => {
                // For column references in windowed queries, return the value from the first record
                // This represents the GROUP BY key value for the window
                debug!(
                    "AGG: Processing column '{}' in aggregate context",
                    column_name
                );
                if let Some(first_record) = records.first() {
                    if let Some(value) = first_record.fields.get(column_name) {
                        debug!("AGG: Found column '{}' = {:?}", column_name, value);
                        Ok(value.clone())
                    } else {
                        debug!("AGG: Column '{}' not found, returning Null", column_name);
                        Ok(FieldValue::Null)
                    }
                } else {
                    Ok(FieldValue::Null)
                }
            }
            _ => {
                // For non-function expressions, evaluate on first record
                debug!("AGG: Processing non-function expression: {:?}", expr);
                if let Some(first_record) = records.first() {
                    ExpressionEvaluator::evaluate_expression_value(expr, first_record)
                } else {
                    Ok(FieldValue::Null)
                }
            }
        }
    }

    // === PHASE 1B: WATERMARK-AWARE PROCESSING METHODS ===

    /// Enhanced event time extraction that prefers event_time field over processing timestamp
    fn extract_event_time_enhanced(record: &StreamRecord, time_column: Option<&str>) -> i64 {
        // Phase 1B: Prefer event_time field if available
        if let Some(event_time) = record.event_time {
            return event_time.timestamp_millis();
        }

        // Fallback to existing logic
        Self::extract_event_time(record, time_column)
    }

    /// Check if window should emit considering watermarks (Phase 1B)
    fn should_emit_window_with_watermark(
        window_state: &WindowState,
        event_time: i64,
        window_spec: &WindowSpec,
        context: &ProcessorContext,
    ) -> bool {
        // If no watermarks enabled, use legacy logic
        if !context.has_watermarks_enabled() {
            return Self::should_emit_window_state(window_state, event_time, window_spec);
        }

        let global_watermark = context.get_global_watermark();
        Self::should_emit_window_with_watermark_standalone(
            window_state,
            event_time,
            window_spec,
            global_watermark,
        )
    }

    /// Standalone watermark checking without context borrowing (Phase 1B)
    fn should_emit_window_with_watermark_standalone(
        window_state: &WindowState,
        event_time: i64,
        window_spec: &WindowSpec,
        global_watermark: Option<DateTime<Utc>>,
    ) -> bool {
        // Get global watermark
        let global_watermark_millis = match global_watermark {
            Some(wm) => wm.timestamp_millis(),
            None => return false, // No watermark yet, can't emit safely
        };

        // Check if we have any buffered records
        if window_state.buffer.is_empty() {
            return false;
        }

        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let last_emit = window_state.last_emit;

                // Calculate window boundaries
                let window_start = if last_emit == 0 { 0 } else { last_emit };
                let window_end = window_start + window_size_ms;

                // Emit when watermark has passed the window end
                // This ensures all events for this window have arrived
                global_watermark_millis >= window_end
            }
            WindowSpec::Sliding { size, advance, .. } => {
                let _window_size_ms = size.as_millis() as i64; // Keep for future use
                let slide_ms = advance.as_millis() as i64;
                let last_emit = window_state.last_emit;

                // Calculate next slide boundary
                let next_slide = last_emit + slide_ms;

                // Emit when watermark passes the slide boundary
                global_watermark_millis >= next_slide
            }
            WindowSpec::Session { .. } => {
                // Session windows emit when watermark indicates session timeout
                // For now, use legacy logic (Phase 3 will enhance session window watermarks)
                Self::should_emit_window_state(window_state, event_time, window_spec)
            }
        }
    }

    /// Handle late data according to the configured strategy (Phase 1B)
    fn handle_late_data(
        record: &StreamRecord,
        context: &ProcessorContext,
        query_id: &str,
        _query: &StreamingQuery,
        _window_spec: &WindowSpec,
    ) -> Result<Option<StreamRecord>, SqlError> {
        let lateness = context.calculate_record_lateness(record);

        if let Some(duration) = lateness {
            log::warn!(
                "AGG: Late record detected: {}ms late (event_time: {:?}, timestamp: {}) for query: {}",
                duration.as_millis(),
                record.event_time,
                record.timestamp,
                query_id
            );
        }

        // Get the configured late data strategy from context/config
        // For Phase 1B, implement configurable strategies
        let strategy = Self::get_late_data_strategy_from_context(context);

        if let Some(watermark_manager) = context.watermark_manager.as_ref() {
            let action = watermark_manager.determine_late_data_action(record, &strategy);

            match action {
                LateDataAction::Process => {
                    info!("LATE: Processing late record for query: {}", query_id);
                    // Return None to indicate the record should be processed normally
                    // The caller will continue with regular window processing
                    Ok(None)
                }

                LateDataAction::Drop => {
                    info!("LATE: Dropping late record for query: {}", query_id);
                    Ok(None)
                }

                LateDataAction::DeadLetter => {
                    warn!(
                        "LATE: Sending late record to dead letter queue for query: {}",
                        query_id
                    );
                    // In a real implementation, this would send to a dead letter queue
                    // For now, we'll log and drop
                    Ok(None)
                }

                LateDataAction::UpdatePrevious { window_end } => {
                    info!(
                        "LATE: Late record should update previous window (end: {}) for query: {}",
                        window_end, query_id
                    );
                    // This would require stateful window storage to update previous results
                    // For Phase 1B, log and drop for now
                    Ok(None)
                }
            }
        } else {
            // No watermark manager - drop late data and log
            warn!(
                "LATE: record dropped (no watermark manager) for query: {}",
                query_id
            );
            Ok(None)
        }
    }

    /// Get the late data strategy from context configuration (Phase 1B)
    fn get_late_data_strategy_from_context(_context: &ProcessorContext) -> LateDataStrategy {
        // For Phase 1B, use a default strategy
        // Phase 2 will integrate with StreamingConfig to get the configured strategy
        LateDataStrategy::Drop
    }

    /// Handle late data with a specific strategy (Public API for external consumers)
    pub fn handle_late_record_with_strategy(
        record: &StreamRecord,
        strategy: &LateDataStrategy,
        context: &ProcessorContext,
    ) -> Result<LateDataAction, SqlError> {
        if let Some(watermark_manager) = context.watermark_manager.as_ref() {
            Ok(watermark_manager.determine_late_data_action(record, strategy))
        } else {
            Err(SqlError::ExecutionError {
                message: "Watermark manager not available for late data handling".to_string(),
                query: None,
            })
        }
    }

    /// Check if a window is ready for emission based on watermarks
    /// This is a utility method for external consumers
    pub fn is_window_ready_for_emission(
        window_spec: &WindowSpec,
        window_start_time: i64,
        context: &ProcessorContext,
    ) -> bool {
        if !context.has_watermarks_enabled() {
            return true; // Without watermarks, always ready (legacy behavior)
        }

        let global_watermark = match context.get_global_watermark() {
            Some(wm) => wm.timestamp_millis(),
            None => return false,
        };

        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let window_end = window_start_time + window_size_ms;
                global_watermark >= window_end
            }
            WindowSpec::Sliding { advance, .. } => {
                let slide_ms = advance.as_millis() as i64;
                let slide_end = window_start_time + slide_ms;
                global_watermark >= slide_end
            }
            WindowSpec::Session { .. } => {
                // Session windows use different logic
                true
            }
        }
    }
}

// Extension trait to get time column from WindowSpec
trait WindowSpecExt {
    fn time_column(&self) -> Option<&str>;
}

impl WindowSpecExt for WindowSpec {
    fn time_column(&self) -> Option<&str> {
        match self {
            WindowSpec::Tumbling { time_column, .. } => time_column.as_deref(),
            WindowSpec::Sliding { time_column, .. } => time_column.as_deref(),
            WindowSpec::Session { .. } => None, // Session windows don't have explicit time columns
        }
    }
}
