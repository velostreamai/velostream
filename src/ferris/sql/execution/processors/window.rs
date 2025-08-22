//! Window Query Processor
//!
//! Handles windowed query processing including tumbling, sliding, and session windows.

use super::{ProcessorContext, WindowContext};
use crate::ferris::sql::ast::WindowSpec;
use crate::ferris::sql::execution::{FieldValue, StreamRecord};
use crate::ferris::sql::{SqlError, StreamingQuery};
use std::collections::HashMap;

/// Window processing utilities
pub struct WindowProcessor;

impl WindowProcessor {
    /// Process a windowed query
    pub fn process_windowed_query(
        _query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<Option<StreamRecord>, SqlError> {
        if let StreamingQuery::Select { window, .. } = query {
            if let Some(window_spec) = window {
                // Extract event time first
                let event_time =
                    Self::extract_event_time(record, window_spec.time_column().as_deref());

                // Add record to buffer first
                if let Some(window_context) = context.window_context.as_mut() {
                    window_context.buffer.push(record.clone());

                    // Check if window should emit
                    let should_emit = Self::should_emit_window(
                        window_context,
                        event_time,
                        window_spec.time_column(),
                    );

                    if should_emit {
                        return Self::process_window_emission(
                            query,
                            window_spec,
                            window_context,
                            event_time,
                        );
                    }
                }

                // No emission this cycle
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

    /// Check if window should emit
    pub fn should_emit_window(
        window_context: &WindowContext,
        event_time: i64,
        _time_column: Option<&str>,
    ) -> bool {
        // Simple heuristic: emit if we have buffered records and enough time has passed
        if window_context.buffer.is_empty() {
            return false;
        }

        // For now, emit every 10 records or every 10 seconds
        let time_threshold = window_context.last_emit + 10000; // 10 seconds
        let record_threshold = 10;

        window_context.buffer.len() >= record_threshold || event_time >= time_threshold
    }

    /// Execute windowed aggregation implementation
    fn execute_windowed_aggregation_impl(
        query: &StreamingQuery,
        windowed_buffer: &[StreamRecord],
    ) -> Result<StreamRecord, SqlError> {
        if windowed_buffer.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "No records after filtering".to_string(),
                query: None,
            });
        }

        // For now, return a simple aggregated result
        // This would integrate with the AggregationEngine in a full implementation
        if let StreamingQuery::Select { .. } = query {
            let mut result_fields = HashMap::new();

            // Simple aggregation - count records
            result_fields.insert(
                "count".to_string(),
                FieldValue::Integer(windowed_buffer.len() as i64),
            );

            // Get timestamp from first record
            let first_record = &windowed_buffer[0];

            Ok(StreamRecord {
                fields: result_fields,
                timestamp: first_record.timestamp,
                offset: first_record.offset,
                partition: first_record.partition,
                headers: first_record.headers.clone(),
            })
        } else {
            Err(SqlError::ExecutionError {
                message: "Invalid query type for windowed aggregation".to_string(),
                query: None,
            })
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
