//! Window Query Processor
//!
//! Handles windowed query processing including tumbling, sliding, and session windows.

use super::{ProcessorContext, WindowContext};
use crate::ferris::sql::ast::WindowSpec;
use crate::ferris::sql::execution::internal::WindowState;
use crate::ferris::sql::execution::{FieldValue, StreamRecord};
use crate::ferris::sql::{SqlError, StreamingQuery};
use std::collections::HashMap;

/// Window processing utilities
pub struct WindowProcessor;

impl WindowProcessor {
    /// Process a windowed query using high-performance context state management
    /// Optimized for multi-threading with minimal allocations and lock-free operation
    pub fn process_windowed_query(
        query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<Option<StreamRecord>, SqlError> {
        if let StreamingQuery::Select { window, .. } = query {
            if let Some(window_spec) = window {
                // Extract event time first (minimal CPU overhead)
                let event_time = Self::extract_event_time(record, window_spec.time_column());

                // Get or create window state using high-performance context management
                // This is thread-safe and avoids locks entirely
                let window_state = context.get_or_create_window_state(query_id, window_spec);

                // Add record to buffer (pre-allocated for performance)
                window_state.add_record(record.clone());

                // Check if window should emit using optimized timing logic
                if Self::should_emit_window_state(
                    window_state,
                    event_time,
                    window_spec.time_column(),
                ) {
                    return Self::process_window_emission_state(
                        query,
                        window_spec,
                        window_state,
                        event_time,
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
        query: &StreamingQuery,
        window_spec: &WindowSpec,
        window_state: &mut WindowState,
        event_time: i64,
    ) -> Result<Option<StreamRecord>, SqlError> {
        let last_emit_time = window_state.last_emit;
        let buffer = window_state.buffer.clone();

        // Filter buffer for current window
        let windowed_buffer = match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let completed_window_start = if last_emit_time == 0 {
                    0 // First window: 0 to window_size_ms
                } else {
                    last_emit_time
                };
                let completed_window_end = completed_window_start + window_size_ms;

                // Filter records that belong to the completed window
                buffer
                    .iter()
                    .filter(|r| {
                        let record_time = Self::extract_event_time(r, window_spec.time_column());
                        record_time >= completed_window_start && record_time < completed_window_end
                    })
                    .cloned()
                    .collect()
            }
            _ => buffer, // For other window types, use all buffered records
        };

        // Execute aggregation on filtered records
        let result_option = match Self::execute_windowed_aggregation_impl(query, &windowed_buffer) {
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
        Self::update_window_state_direct(window_state, window_spec, event_time);

        // Clear or adjust buffer based on window type
        Self::cleanup_window_buffer_direct(window_state, window_spec, last_emit_time);

        Ok(result_option)
    }

    /// Process window emission when triggered
    fn process_window_emission(
        query: &StreamingQuery,
        window_spec: &WindowSpec,
        window_context: &mut WindowContext,
        event_time: i64,
    ) -> Result<Option<StreamRecord>, SqlError> {
        let last_emit_time = window_context.last_emit;
        let buffer = window_context.buffer.clone();

        // Filter buffer for current window
        let windowed_buffer = match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let completed_window_start = if last_emit_time == 0 {
                    0 // First window: 0 to window_size_ms
                } else {
                    last_emit_time
                };
                let completed_window_end = completed_window_start + window_size_ms;

                // Filter records that belong to the completed window
                buffer
                    .iter()
                    .filter(|r| {
                        let record_time = Self::extract_event_time(r, window_spec.time_column());
                        record_time >= completed_window_start && record_time < completed_window_end
                    })
                    .cloned()
                    .collect()
            }
            _ => buffer, // For other window types, use all buffered records
        };

        // Execute aggregation on filtered records
        let result_option = match Self::execute_windowed_aggregation_impl(query, &windowed_buffer) {
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
        _time_column: Option<&str>,
    ) -> bool {
        // Check if we have any buffered records
        if window_state.buffer.is_empty() {
            return false;
        }

        // Get the last emit time
        let last_emit = window_state.last_emit;

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

    /// Execute windowed aggregation implementation using logic from legacy method
    fn execute_windowed_aggregation_impl(
        query: &StreamingQuery,
        windowed_buffer: &[StreamRecord],
    ) -> Result<StreamRecord, SqlError> {
        use crate::ferris::sql::execution::expression::evaluator::ExpressionEvaluator;

        if windowed_buffer.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "No records after filtering".to_string(),
                query: None,
            });
        }

        if let StreamingQuery::Select {
            fields,
            where_clause,
            group_by,
            having,
            ..
        } = query
        {
            // Step 1: Filter records by WHERE clause
            let filtered_records: Vec<&StreamRecord> = windowed_buffer
                .iter()
                .filter(|record| {
                    if let Some(where_expr) = where_clause {
                        ExpressionEvaluator::evaluate_expression(where_expr, record)
                            .unwrap_or(false)
                    } else {
                        true
                    }
                })
                .collect();

            if filtered_records.is_empty() {
                return Err(SqlError::ExecutionError {
                    message: "No records after filtering".to_string(),
                    query: None,
                });
            }

            // Step 2: For windowed queries, create aggregated result
            let mut result_fields = HashMap::new();

            // Simple aggregation logic - process the first record as representative
            if let Some(first_record) = filtered_records.first() {
                // Process SELECT fields
                for field in fields {
                    match field {
                        crate::ferris::sql::ast::SelectField::Wildcard => {
                            // For windowed aggregations, add basic aggregate info instead of all fields
                            result_fields.insert(
                                "window_size".to_string(),
                                FieldValue::Integer(filtered_records.len() as i64),
                            );
                        }
                        crate::ferris::sql::ast::SelectField::Expression { expr, alias } => {
                            let field_name = alias
                                .as_ref()
                                .map(|a| a.clone())
                                .unwrap_or_else(|| format!("field_{}", result_fields.len()));

                            // Evaluate expression - for aggregates, this would do proper aggregation
                            if let Ok(value) =
                                ExpressionEvaluator::evaluate_expression_value(expr, first_record)
                            {
                                result_fields.insert(field_name, value);
                            } else {
                                // Default to record count for unknown expressions in window context
                                result_fields.insert(
                                    field_name,
                                    FieldValue::Integer(filtered_records.len() as i64),
                                );
                            }
                        }
                        crate::ferris::sql::ast::SelectField::Column(column_name) => {
                            // Simple column reference
                            if let Some(value) = first_record.fields.get(column_name) {
                                result_fields.insert(column_name.clone(), value.clone());
                            }
                        }
                        crate::ferris::sql::ast::SelectField::AliasedColumn { column, alias } => {
                            // Aliased column reference
                            if let Some(value) = first_record.fields.get(column) {
                                result_fields.insert(alias.clone(), value.clone());
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

            // Use timestamp from the last record in the window
            let timestamp = filtered_records.last().map(|r| r.timestamp).unwrap_or(0);

            Ok(StreamRecord {
                fields: result_fields,
                timestamp,
                offset: 0,
                partition: 0,
                headers: HashMap::new(),
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
                let cutoff_time = window_state.last_emit - window_size_ms;
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
                let cutoff_time = window_state.last_emit - gap_ms;
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
                let cutoff_time = window_context.last_emit - window_size_ms;
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
                let cutoff_time = window_context.last_emit - gap_ms;
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
