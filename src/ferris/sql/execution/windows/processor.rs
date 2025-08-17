//! Window processor implementation for streaming SQL window operations.

use crate::ferris::sql::ast::{StreamingQuery, WindowSpec};
use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::groupby::GroupByProcessor;
use crate::ferris::sql::execution::types::StreamRecord;

use super::state::WindowState;

/// Processor for window operations in streaming SQL queries
///
/// This processor handles all aspects of window-based processing including:
/// - Record buffering and eviction based on window specifications
/// - Window emission timing and boundary calculations
/// - Integration with GROUP BY operations for windowed aggregation
/// - Support for all window types (tumbling, sliding, session)
pub struct WindowProcessor;

impl WindowProcessor {
    /// Process a record through a windowed query
    pub fn process_record(state: &mut WindowState, record: &StreamRecord) -> Result<(), SqlError> {
        // Extract event time from the record
        let event_time = Self::extract_event_time(record, state.get_time_column());

        // Add record to the window buffer
        state
            .add_record(record.clone())
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to add record to window buffer: {}", e),
                query: None,
            })?;

        // Evict old records that are outside the window
        state.evict_old_records(event_time);

        Ok(())
    }

    /// Check if window should emit based on window specification and current time
    pub fn should_emit_window(state: &WindowState, current_time: i64) -> bool {
        match &state.window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let current_window_start =
                    Self::align_to_window_boundary(current_time, window_size_ms);

                // For tumbling windows, only emit when:
                // 1. This is the first window (last_emit == 0) AND we have the first record past window size
                // 2. We've crossed into a new window boundary
                if state.last_emit == 0 {
                    // First window - only emit if we have records past the first window boundary
                    current_time >= window_size_ms
                } else {
                    // Subsequent windows - emit when we cross window boundaries
                    current_window_start > state.last_emit
                }
            }
            WindowSpec::Sliding { advance, .. } => {
                let advance_ms = advance.as_millis() as i64;
                current_time >= state.last_emit + advance_ms
            }
            WindowSpec::Session { gap, .. } => {
                let gap_ms = gap.as_millis() as i64;
                // Session window emits when gap timeout is exceeded
                current_time >= state.last_emit + gap_ms
            }
        }
    }

    /// Get records for the current window that should be emitted
    pub fn get_window_records_for_emission(
        state: &WindowState,
        current_time: i64,
    ) -> Vec<StreamRecord> {
        match &state.window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let completed_window_start = if state.last_emit == 0 {
                    0 // First window: 0 to window_size_ms
                } else {
                    state.last_emit
                };
                let completed_window_end = completed_window_start + window_size_ms;

                // Filter records that belong to the completed window
                state
                    .get_window_records()
                    .iter()
                    .filter(|r| {
                        let record_time = Self::extract_event_time(r, state.get_time_column());
                        record_time >= completed_window_start && record_time < completed_window_end
                    })
                    .cloned()
                    .collect()
            }
            WindowSpec::Sliding { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                let window_start = current_time - window_size_ms;

                // Filter records that belong to the sliding window
                state
                    .get_window_records()
                    .iter()
                    .filter(|r| {
                        let record_time = Self::extract_event_time(r, state.get_time_column());
                        record_time >= window_start && record_time <= current_time
                    })
                    .cloned()
                    .collect()
            }
            WindowSpec::Session { .. } => {
                // For session windows, return all records in the current session
                state.get_window_records().iter().cloned().collect()
            }
        }
    }

    /// Execute windowed aggregation on a set of records
    pub fn execute_windowed_aggregation(
        query: &StreamingQuery,
        records: &[StreamRecord],
    ) -> Result<StreamRecord, SqlError> {
        match query {
            StreamingQuery::Select {
                fields, group_by, ..
            } => {
                if let Some(group_exprs) = group_by {
                    // Handle GROUP BY in windowed context
                    Self::execute_windowed_group_by(fields, "stream", group_exprs, records)
                } else {
                    // Simple aggregation without GROUP BY
                    Self::execute_simple_windowed_aggregation(fields, "stream", records)
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: "Unsupported query type for windowed aggregation".to_string(),
                query: None,
            }),
        }
    }

    /// Execute windowed GROUP BY aggregation
    fn execute_windowed_group_by(
        fields: &[crate::ferris::sql::ast::SelectField],
        _from: &str,
        group_exprs: &[crate::ferris::sql::ast::Expr],
        records: &[StreamRecord],
    ) -> Result<StreamRecord, SqlError> {
        use crate::ferris::sql::execution::groupby::GroupByState;
        use std::collections::HashMap;

        // Create a temporary GROUP BY state
        let mut group_state = GroupByState::new(
            group_exprs.to_vec(),
            fields.to_vec(),
            None, // No HAVING clause for now
        );

        // Process all records through the GROUP BY
        for record in records {
            GroupByProcessor::process_record(&mut group_state, record)?;
        }

        // Get the first group's result (for windowed queries, we typically expect one group)
        if let Some(group_key) = group_state.group_keys().next() {
            if let Some(accumulator) = group_state.get_accumulator(group_key) {
                let sample_record = accumulator.sample_record.as_ref();
                let result_fields = GroupByProcessor::compute_group_result_fields(
                    accumulator,
                    fields,
                    group_exprs,
                    sample_record,
                )?;

                // Create result record
                Ok(StreamRecord {
                    fields: result_fields,
                    timestamp: records.last().map(|r| r.timestamp).unwrap_or(0),
                    offset: records.last().map(|r| r.offset).unwrap_or(0),
                    partition: records.last().map(|r| r.partition).unwrap_or(0),
                    headers: HashMap::new(),
                })
            } else {
                Err(SqlError::ExecutionError {
                    message: "No accumulator found for group".to_string(),
                    query: None,
                })
            }
        } else {
            // No groups found - create empty result
            Ok(StreamRecord {
                fields: HashMap::new(),
                timestamp: 0,
                offset: 0,
                partition: 0,
                headers: HashMap::new(),
            })
        }
    }

    /// Execute simple windowed aggregation (without GROUP BY)
    fn execute_simple_windowed_aggregation(
        fields: &[crate::ferris::sql::ast::SelectField],
        _from: &str,
        records: &[StreamRecord],
    ) -> Result<StreamRecord, SqlError> {
        use crate::ferris::sql::execution::groupby::{GroupAccumulator, GroupByProcessor};
        use std::collections::HashMap;

        if records.is_empty() {
            return Ok(StreamRecord {
                fields: HashMap::new(),
                timestamp: 0,
                offset: 0,
                partition: 0,
                headers: HashMap::new(),
            });
        }

        // Create a single accumulator for all records
        let mut accumulator = GroupAccumulator::new();

        // Process all records
        for record in records {
            GroupByProcessor::update_group_accumulator(&mut accumulator, record, fields)?;
        }

        // Compute result fields
        let result_fields = GroupByProcessor::compute_group_result_fields(
            &accumulator,
            fields,
            &[], // No group expressions
            accumulator.sample_record.as_ref(),
        )?;

        // Create result record
        Ok(StreamRecord {
            fields: result_fields,
            timestamp: records.last().unwrap().timestamp,
            offset: records.last().unwrap().offset,
            partition: records.last().unwrap().partition,
            headers: HashMap::new(),
        })
    }

    /// Extract event time from a record
    pub fn extract_event_time(record: &StreamRecord, time_column: Option<&str>) -> i64 {
        if let Some(time_col) = time_column {
            // Use specified time column
            if let Some(field_value) = record.fields.get(time_col) {
                match field_value {
                    crate::ferris::sql::execution::types::FieldValue::Integer(ts) => *ts,
                    crate::ferris::sql::execution::types::FieldValue::Timestamp(ts) => {
                        ts.and_utc().timestamp_millis()
                    }
                    _ => record.timestamp, // Fallback to record timestamp
                }
            } else {
                record.timestamp
            }
        } else {
            // Use record's built-in timestamp
            record.timestamp
        }
    }

    /// Align timestamp to window boundary
    pub fn align_to_window_boundary(timestamp: i64, window_size_ms: i64) -> i64 {
        (timestamp / window_size_ms) * window_size_ms
    }

    /// Calculate the window start time for a given timestamp
    pub fn calculate_window_start(timestamp: i64, window_spec: &WindowSpec) -> i64 {
        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                Self::align_to_window_boundary(timestamp, window_size_ms)
            }
            WindowSpec::Sliding { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                timestamp - window_size_ms
            }
            WindowSpec::Session { .. } => {
                // Session windows start with the first event
                timestamp
            }
        }
    }

    /// Calculate the window end time for a given timestamp
    pub fn calculate_window_end(timestamp: i64, window_spec: &WindowSpec) -> i64 {
        match window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                Self::align_to_window_boundary(timestamp, window_size_ms) + window_size_ms
            }
            WindowSpec::Sliding { size, .. } => {
                let window_size_ms = size.as_millis() as i64;
                timestamp + window_size_ms
            }
            WindowSpec::Session { gap, .. } => {
                let gap_ms = gap.as_millis() as i64;
                timestamp + gap_ms
            }
        }
    }

    /// Check if a record belongs to a specific window
    pub fn record_belongs_to_window(
        record: &StreamRecord,
        window_start: i64,
        window_end: i64,
        time_column: Option<&str>,
    ) -> bool {
        let record_time = Self::extract_event_time(record, time_column);
        record_time >= window_start && record_time < window_end
    }

    /// Get the watermark for late data handling
    pub fn calculate_watermark(latest_timestamp: i64, max_lateness_ms: i64) -> i64 {
        latest_timestamp - max_lateness_ms
    }
}
