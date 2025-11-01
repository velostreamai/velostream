//! Window Query Processor
//!
//! Handles windowed query processing including tumbling, sliding, and session windows.
//! Phase 1B enhancement: Added watermark-aware processing for proper event-time semantics.

use super::{ProcessorContext, WindowContext};
use crate::velostream::sql::ast::{RowsEmitMode, WindowSpec};
use crate::velostream::sql::execution::expression::{ExpressionEvaluator, SelectAliasContext};
use crate::velostream::sql::execution::internal::{GroupAccumulator, RowsWindowState, WindowState};
use crate::velostream::sql::execution::validation::{FieldValidator, ValidationContext};
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

                // Phase 7: For EMIT CHANGES, emit per-record state changes
                let is_emit_changes = Self::is_emit_changes(query);
                let group_by_cols = Self::get_group_by_columns(query);

                // Check if window should emit
                // For EMIT CHANGES with GROUP BY: emit on every record
                // For others: emit only on window boundaries
                // CRITICAL FIX: Group BY queries WITHOUT explicit EMIT CHANGES should use window boundaries
                let should_emit = if is_emit_changes && group_by_cols.is_some() {
                    true
                } else {
                    // For all other cases (GROUP BY without EMIT CHANGES, or non-GROUP BY queries),
                    // only emit when window boundary is reached
                    Self::should_emit_window_state(window_state, event_time, window_spec)
                };

                if should_emit {
                    // Drop window_state borrow before calling process_window_emission_state
                    let result = Self::process_window_emission_state(
                        query_id,
                        query,
                        window_spec,
                        event_time,
                        context,
                    );
                    return result;
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
        // Phase 1: GROUP BY Detection for Window combination
        // Check if this is a GROUP BY windowed query (with or without EMIT CHANGES)
        let group_by_cols = Self::get_group_by_columns(query);
        let is_emit_changes = Self::is_emit_changes(query);

        // Phase 3: ENGINE INTEGRATION - Route ALL GROUP BY queries through multi-group path
        // CRITICAL: execute_windowed_aggregation_impl() ignores GROUP BY, so we must use
        // compute_all_group_results() for any query with GROUP BY
        if let Some(cols) = &group_by_cols {
            debug!(
                "FR-079 Phase 3: Activating GROUP BY windowed query routing (EMIT CHANGES: {}) for query: {}",
                is_emit_changes, query_id
            );

            // Get window state reference - borrow will be released after cloning buffer
            let window_state = context.get_or_create_window_state(query_id, window_spec);
            let last_emit_time = window_state.last_emit;
            let buffer = window_state.buffer.clone();
            // window_state borrow ends here

            // Filter buffer for current window
            // For EMIT CHANGES with GROUP BY, use all buffered records (no window boundary filtering)
            // For standard window emissions, filter by window completion
            let is_emit_changes = Self::is_emit_changes(query);
            let (windowed_buffer, window_start, window_end) = if is_emit_changes {
                // EMIT CHANGES: Use ALL records in buffer, don't filter by window boundary
                debug!("FR-079 Phase 7: EMIT CHANGES mode - using all buffered records");
                (buffer.clone(), 0, 0)
            } else {
                // Standard window emission: Filter by window completion
                match window_spec {
                    WindowSpec::Tumbling { size, .. } => {
                        let window_size_ms = size.as_millis() as i64;
                        let start = if last_emit_time == 0 {
                            0 // First window: 0 to window_size_ms
                        } else {
                            last_emit_time
                        };
                        let end = start + window_size_ms;
                        // Filter records that belong to the completed window
                        let filtered = buffer
                            .iter()
                            .filter(|r| {
                                let record_time =
                                    Self::extract_event_time(r, window_spec.time_column());
                                record_time >= start && record_time < end
                            })
                            .cloned()
                            .collect();
                        (filtered, start, end)
                    }
                    WindowSpec::Sliding { size, .. } => {
                        // CRITICAL FIX: Filter records within the sliding window bounds
                        // For SLIDING windows, include records from [start, end] (inclusive on both ends)
                        let window_size_ms = size.as_millis() as i64;
                        let window_end = event_time;
                        let window_start = window_end - window_size_ms;

                        let filtered = buffer
                            .iter()
                            .filter(|r| {
                                let record_time =
                                    Self::extract_event_time(r, window_spec.time_column());
                                // Include current record: use <= instead of <
                                record_time >= window_start && record_time <= window_end
                            })
                            .cloned()
                            .collect();
                        (filtered, window_start, window_end)
                    }
                    WindowSpec::Session { .. } => (buffer.clone(), 0, 0),
                    WindowSpec::Rows { .. } => (buffer.clone(), 0, 0), // Phase 8.2: ROWS windows - emit all buffered records
                }
            };

            // Phase 3/4: Compute ALL group results and queue for emission
            let all_results = Self::compute_all_group_results(
                cols,
                &windowed_buffer,
                query,
                window_spec,
                context,
                window_start,
                window_end,
            )?;

            if all_results.is_empty() {
                debug!(
                    "⚠️  FR-079 Phase 7: No group results from aggregation - all groups may have failed"
                );
                debug!("FR-079 Phase 3/4: No group results from aggregation");
                // Update window state even if no results
                let window_state = context.get_or_create_window_state(query_id, window_spec);
                Self::update_window_state_direct(window_state, window_spec, event_time);
                // FR-079 Phase 7 FIX: Don't cleanup for EMIT CHANGES
                // We need to keep the buffer intact to re-emit state changes on every record
                return Ok(None);
            }

            debug!(
                "FR-079 Phase 4: Computed {} group results for emission",
                all_results.len()
            );

            // Phase 4: Queue additional results (2nd, 3rd, etc.) for emission in subsequent cycles
            let mut result_iter = all_results.into_iter();
            let first_result = result_iter
                .next()
                .expect("all_results is not empty, already checked");
            let remaining_results: Vec<_> = result_iter.collect();

            if !remaining_results.is_empty() {
                context.queue_results(query_id, remaining_results.clone());
                debug!(
                    "FR-079 Phase 4: Queued {} additional group results for later emission",
                    remaining_results.len()
                );
            }

            // Update window state after aggregation
            let window_state = context.get_or_create_window_state(query_id, window_spec);
            let last_emit_time_before_update = window_state.last_emit;
            Self::update_window_state_direct(window_state, window_spec, event_time);

            // FR-079 Phase 7 FIX: Only cleanup buffer for standard (non-EMIT CHANGES) queries
            // For EMIT CHANGES, we keep the buffer intact to re-emit state changes on every record
            if !is_emit_changes {
                // For standard GROUP BY queries, cleanup emitted records to prevent duplicate emissions
                let window_state = context.get_or_create_window_state(query_id, window_spec);
                Self::cleanup_window_buffer_direct(
                    window_state,
                    window_spec,
                    last_emit_time_before_update,
                );
            }

            Ok(Some(first_result))
        } else {
            // Original single-result path (non-GROUP BY + EMIT CHANGES queries)
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
                WindowSpec::Rows { buffer_size, .. } => {
                    // Phase 8.2: ROWS windows - bounds based on buffer count
                    let current_buffer_len = buffer.len() as i64;
                    (0, current_buffer_len.min(*buffer_size as i64))
                }
            };

            // Filter buffer for current window
            let windowed_buffer = match window_spec {
                WindowSpec::Tumbling { .. } => {
                    // Filter records that belong to the completed window
                    buffer
                        .iter()
                        .filter(|r| {
                            let record_time =
                                Self::extract_event_time(r, window_spec.time_column());
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
            WindowSpec::Rows { buffer_size, .. } => {
                // Phase 8.2: ROWS windows - bounds based on buffer count
                let current_buffer_len = buffer.len() as i64;
                (0, current_buffer_len.min(*buffer_size as i64))
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
    /// IMPORTANT: All timestamps must be in milliseconds since epoch
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
                let should_emit = if last_emit == 0 {
                    event_time >= advance_ms
                } else {
                    event_time >= last_emit + advance_ms
                };

                should_emit
            }
            WindowSpec::Session { .. } => {
                // Session windows use enhanced logic with proper gap detection and overflow safety
                // For now, emit after we have at least one record to enable basic functionality
                !window_state.buffer.is_empty()
            }
            WindowSpec::Rows { emit_mode, .. } => {
                // Phase 8.2: ROWS windows - emit based on mode (EveryRecord or BufferFull)
                match emit_mode {
                    RowsEmitMode::EveryRecord => !window_state.buffer.is_empty(),
                    RowsEmitMode::BufferFull => false, // Will be implemented in buffer mgmt
                }
            }
        }
    }

    /// Extract GROUP BY columns from query (Phase 1: GROUP BY Detection)
    ///
    /// Returns Some(Vec<String>) with the GROUP BY column names if GROUP BY exists,
    /// None otherwise. Only supports simple column references in GROUP BY.
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Query: SELECT status, COUNT(*) FROM orders GROUP BY status
    /// let group_by_cols = WindowProcessor::get_group_by_columns(query);
    /// assert_eq!(group_by_cols, Some(vec!["status".to_string()]));
    /// ```
    pub fn get_group_by_columns(query: &StreamingQuery) -> Option<Vec<String>> {
        if let StreamingQuery::Select {
            group_by: Some(exprs),
            ..
        } = query
        {
            exprs
                .iter()
                .map(|expr| {
                    match expr {
                        crate::velostream::sql::ast::Expr::Column(col_name) => {
                            Some(col_name.clone())
                        }
                        _ => {
                            None // Only support simple column references
                        }
                    }
                })
                .collect::<Option<Vec<_>>>()
        } else {
            None
        }
    }

    /// Check if query uses EMIT CHANGES mode (Phase 1: GROUP BY Detection)
    ///
    /// Returns true if the query has EMIT CHANGES mode set, false for EMIT FINAL or no emit mode.
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Query: SELECT status, COUNT(*) FROM orders EMIT CHANGES
    /// let is_changes = WindowProcessor::is_emit_changes(query);
    /// assert_eq!(is_changes, true);
    /// ```
    pub fn is_emit_changes(query: &StreamingQuery) -> bool {
        if let StreamingQuery::Select { emit_mode, .. } = query {
            matches!(
                emit_mode,
                Some(crate::velostream::sql::ast::EmitMode::Changes)
            )
        } else {
            false
        }
    }

    /// Extract GROUP BY key from a record (Phase 2: Group Splitting)
    ///
    /// Extracts the values of GROUP BY columns from a record to form a group key.
    /// Returns a Vec<FieldValue> representing the group key for matching.
    ///
    /// # Examples
    /// ```rust,no_run
    /// // For query: SELECT status, COUNT(*) FROM orders GROUP BY status
    /// // Record: {id: 1, status: "pending", amount: 100}
    /// // Result: vec![FieldValue::String("pending")]
    /// ```
    fn extract_group_key(
        record: &StreamRecord,
        group_by_cols: &[String],
    ) -> Result<Vec<FieldValue>, SqlError> {
        // Phase 2: Validate that all GROUP BY fields exist in the record
        FieldValidator::validate_fields_exist(
            record,
            &group_by_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            ValidationContext::GroupBy,
        )
        .map_err(|e| e.to_sql_error())?;

        // Extract field values for GROUP BY columns
        let group_key = group_by_cols
            .iter()
            .map(|col_name| {
                record
                    .fields
                    .get(col_name)
                    .cloned()
                    .unwrap_or(FieldValue::String("NULL".to_string()))
            })
            .collect();

        Ok(group_key)
    }

    /// Split window buffer into groups by GROUP BY columns (Phase 2: Group Splitting)
    ///
    /// Partitions records by their GROUP BY key values, similar to Flink's keyBy() operation.
    /// Uses string representation of keys since FieldValue doesn't implement Eq+Hash.
    /// Returns a HashMap where keys are string representations and values are vectors of records in that group.
    ///
    /// # Examples
    /// ```rust,no_run
    /// // Input: 5 records (3 "pending", 2 "completed")
    /// // Output: HashMap with 2 entries:
    /// //   "pending" → [rec1, rec2, rec4]
    /// //   "completed" → [rec3, rec5]
    /// ```
    fn split_buffer_by_groups(
        records: &[StreamRecord],
        group_by_cols: &[String],
    ) -> Result<HashMap<String, (Vec<FieldValue>, Vec<StreamRecord>)>, SqlError> {
        let mut groups: HashMap<String, (Vec<FieldValue>, Vec<StreamRecord>)> = HashMap::new();

        for record in records {
            let group_key = Self::extract_group_key(record, group_by_cols)?;
            // Use debug representation as HashMap key (safe because FieldValue is Eq semantically)
            let key_str = format!("{:?}", group_key);
            groups
                .entry(key_str)
                .or_insert_with(|| (group_key.clone(), Vec::new()))
                .1
                .push(record.clone());
        }

        Ok(groups)
    }

    /// Compute aggregation result for a single GROUP BY group (Phase 2: Per-Group Aggregation)
    ///
    /// Applies the same aggregation logic as execute_windowed_aggregation_impl but for a single group.
    /// Includes the GROUP BY column values in the result.
    ///
    /// # Parameters
    /// - `group_key`: Vec of FieldValues representing the GROUP BY key
    /// - `group_records`: Records belonging to this group
    /// - `query`: The streaming query with SELECT/WHERE/HAVING expressions
    /// - `window_spec`: Window specification for boundaries
    /// - `context`: Processor context for EXISTS subquery support
    ///
    /// # Returns
    /// A StreamRecord with aggregated values for this group, or SqlError if computation fails
    fn compute_group_aggregate(
        group_key: &[FieldValue],
        group_records: &[StreamRecord],
        query: &StreamingQuery,
        window_spec: &WindowSpec,
        context: &ProcessorContext,
        group_by_cols: &[String],
        window_start: i64,
        window_end: i64,
    ) -> Result<StreamRecord, SqlError> {
        // Execute aggregation on this group using the existing logic
        // Pass the correct window boundaries to ensure _window_start and _window_end are set correctly
        let mut result = Self::execute_windowed_aggregation_impl(
            query,
            group_records,
            window_start,
            window_end,
            context,
        )?;

        // Prepend GROUP BY columns to the result (at the beginning)
        // This ensures GROUP BY columns appear first in result
        let mut group_by_fields: HashMap<String, FieldValue> = HashMap::new();
        for (i, col_name) in group_by_cols.iter().enumerate() {
            if let Some(key_val) = group_key.get(i) {
                group_by_fields.insert(col_name.clone(), key_val.clone());
            }
        }

        // Merge GROUP BY fields with aggregation results
        // GROUP BY fields take precedence (appear first)
        let mut final_fields = group_by_fields;
        for (key, value) in result.fields {
            // Don't override GROUP BY columns
            if !final_fields.contains_key(&key) {
                final_fields.insert(key, value);
            }
        }

        result.fields = final_fields;
        Ok(result)
    }

    /// Compute all group results for GROUP BY queries (Phase 3: Multi-Result Collection)
    ///
    /// This function computes aggregations for ALL groups, returning all results.
    /// Used to collect results that will be queued for emission.
    ///
    /// # Returns
    /// Vec<StreamRecord> containing aggregation results for all groups
    fn compute_all_group_results(
        group_by_cols: &[String],
        buffer: &[StreamRecord],
        query: &StreamingQuery,
        window_spec: &WindowSpec,
        context: &ProcessorContext,
        window_start: i64,
        window_end: i64,
    ) -> Result<Vec<StreamRecord>, SqlError> {
        debug!(
            "FR-079 Phase 3: compute_all_group_results called with {} records, {} GROUP BY columns, window=[{}..{}]",
            buffer.len(),
            group_by_cols.len(),
            window_start,
            window_end
        );

        if buffer.is_empty() {
            debug!("FR-079 Phase 3: Empty buffer, no groups to process");
            return Ok(Vec::new());
        }

        // Step 1: Split buffer into groups
        let groups = Self::split_buffer_by_groups(buffer, group_by_cols)?;
        debug!(
            "FR-079 Phase 3: Split buffer into {} distinct groups",
            groups.len()
        );

        if groups.is_empty() {
            return Ok(Vec::new());
        }

        // Step 2: Compute aggregations for each group
        let mut all_results = Vec::new();
        for (_key_str, (group_key, group_records)) in groups.iter() {
            debug!(
                "FR-079 Phase 3: Computing aggregate for group {:?} with {} records",
                group_key,
                group_records.len()
            );

            match Self::compute_group_aggregate(
                group_key,
                group_records,
                query,
                window_spec,
                context,
                group_by_cols,
                window_start,
                window_end,
            ) {
                Ok(result) => {
                    debug!(
                        "FR-079 Phase 3: Successfully computed aggregate for group {:?}",
                        group_key
                    );
                    all_results.push(result);
                }
                Err(e) => {
                    debug!(
                        "⚠️  FR-079 Phase 3: Error computing aggregate for group {:?}: {}",
                        group_key, e
                    );
                    debug!(
                        "FR-079 Phase 3: Error computing aggregate for group {:?}: {:?}",
                        group_key, e
                    );
                    // Emit partial result with GROUP BY columns AND window metadata when aggregation fails
                    let mut partial_fields = HashMap::new();
                    for (i, col_name) in group_by_cols.iter().enumerate() {
                        if let Some(key_val) = group_key.get(i) {
                            partial_fields.insert(col_name.clone(), key_val.clone());
                        }
                    }

                    // Add window metadata to partial result
                    partial_fields.insert(
                        "_window_start".to_string(),
                        FieldValue::Integer(window_start),
                    );
                    partial_fields
                        .insert("_window_end".to_string(), FieldValue::Integer(window_end));

                    let partial_result = StreamRecord {
                        fields: partial_fields,
                        headers: HashMap::new(),
                        event_time: None,
                        timestamp: 0,
                        offset: 0,
                        partition: 0,
                    };
                    all_results.push(partial_result);
                }
            }
        }

        debug!(
            "FR-079 Phase 3: Collected {} group results",
            all_results.len()
        );

        Ok(all_results)
    }

    /// Process windowed GROUP BY query with EMIT CHANGES (Phase 2: Main Orchestration)
    ///
    /// This function implements the complete GROUP BY + EMIT CHANGES flow:
    /// 1. Split buffer into groups by GROUP BY columns
    /// 2. Compute per-group aggregations
    /// 3. Return first result (caller will queue additional results in context for Phase 3)
    ///
    /// # Returns
    /// Returns the first group's result, or None if no groups exist.
    /// Additional group results should be queued for emission in Phase 3.
    fn process_windowed_group_by_emission(
        group_by_cols: &[String],
        buffer: &[StreamRecord],
        query: &StreamingQuery,
        window_spec: &WindowSpec,
        context: &ProcessorContext,
        window_start: i64,
        window_end: i64,
    ) -> Result<Option<StreamRecord>, SqlError> {
        debug!(
            "FR-079 Phase 2: process_windowed_group_by_emission called with {} records, {} GROUP BY columns",
            buffer.len(),
            group_by_cols.len()
        );

        if buffer.is_empty() {
            debug!("⚠️  FR-079 Phase 7: Empty buffer, no groups to process");
            debug!("FR-079 Phase 2: Empty buffer, no groups to process");
            return Ok(None);
        }

        // Step 1: Split buffer into groups
        let groups = Self::split_buffer_by_groups(buffer, group_by_cols)?;
        debug!(
            "FR-079 Phase 2: Split buffer into {} distinct groups",
            groups.len()
        );

        if groups.is_empty() {
            debug!("⚠️  FR-079 Phase 7: Failed to split buffer into groups");
            debug!("FR-079 Phase 2: No distinct groups found from buffer");
            return Ok(None);
        }

        // Step 2: Compute aggregations for each group
        let mut results = Vec::new();
        for (_key_str, (group_key, group_records)) in groups.iter() {
            debug!(
                "FR-079 Phase 2: Computing aggregate for group {:?} with {} records",
                group_key,
                group_records.len()
            );

            match Self::compute_group_aggregate(
                group_key,
                group_records,
                query,
                window_spec,
                context,
                group_by_cols,
                window_start,
                window_end,
            ) {
                Ok(result) => {
                    debug!(
                        "FR-079 Phase 2: Successfully computed aggregate for group {:?}",
                        group_key
                    );
                    results.push(result);
                }
                Err(e) => {
                    debug!(
                        "⚠️  FR-079 Phase 7: Error computing aggregate for group {:?}: {}",
                        group_key, e
                    );
                    debug!(
                        "FR-079 Phase 7: Error computing aggregate for group {:?}: {:?}",
                        group_key, e
                    );
                    debug!(
                        "FR-079 Phase 7: Will emit partial result with {} GROUP BY columns AND window metadata: {:?}",
                        group_by_cols.len(),
                        group_by_cols
                    );
                    // Emit partial result with GROUP BY columns AND window metadata when aggregation fails
                    let mut partial_fields = HashMap::new();
                    for (i, col_name) in group_by_cols.iter().enumerate() {
                        if let Some(key_val) = group_key.get(i) {
                            partial_fields.insert(col_name.clone(), key_val.clone());
                        }
                    }

                    // Add window metadata to partial result
                    partial_fields.insert(
                        "_window_start".to_string(),
                        FieldValue::Integer(window_start),
                    );
                    partial_fields
                        .insert("_window_end".to_string(), FieldValue::Integer(window_end));

                    debug!(
                        "FR-079 Phase 7: Emitting partial result with {} fields",
                        partial_fields.len()
                    );

                    let partial_result = StreamRecord {
                        fields: partial_fields,
                        headers: HashMap::new(),
                        event_time: None,
                        timestamp: 0,
                        offset: 0,
                        partition: 0,
                    };
                    results.push(partial_result);
                }
            }
        }

        // Step 3: Return first result (Phase 3 will handle multi-result emission)
        // For now, return the first result to maintain backward compatibility
        // Phase 3 will implement Vec<StreamRecord> return type for all results
        if !results.is_empty() {
            debug!(
                "FR-079 Phase 2: Returning first of {} group results",
                results.len()
            );
            // TODO: Phase 3 will queue remaining results in context for emission
            Ok(Some(results.into_iter().next().unwrap()))
        } else {
            debug!("FR-079 Phase 2: No valid results after aggregation");
            Ok(None)
        }
    }

    /// FR-079 Phase 6: Build GroupAccumulator from records for aggregate expression evaluation
    ///
    /// This helper function constructs a GroupAccumulator containing all numeric values from
    /// the records, enabling proper calculation of statistical functions like STDDEV and VARIANCE
    /// in aggregate expressions.
    fn build_accumulator_from_records(records: &[&StreamRecord]) -> GroupAccumulator {
        let mut accumulator = GroupAccumulator::new();

        for record in records {
            accumulator.increment_count();

            // Extract numeric values for all fields in the record
            // This populates numeric_values HashMap used by STDDEV/VARIANCE functions
            for (field_name, value) in &record.fields {
                match value {
                    FieldValue::Float(f) => {
                        accumulator.add_value_for_stats(field_name, *f);
                    }
                    FieldValue::Integer(i) => {
                        accumulator.add_value_for_stats(field_name, *i as f64);
                    }
                    FieldValue::ScaledInteger(val, scale) => {
                        // Convert scaled integer to f64
                        let float_val = *val as f64 / 10_i64.pow(*scale as u32) as f64;
                        accumulator.add_value_for_stats(field_name, float_val);
                    }
                    _ => {
                        // Skip non-numeric values
                    }
                }
            }
        }

        accumulator
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
            // Create alias context for storing computed field values during SELECT processing
            let mut agg_alias_context = SelectAliasContext::new();

            // FR-079 Phase 6: Build accumulator with numeric values from all filtered records
            // This enables proper calculation of statistical functions like STDDEV and VARIANCE
            let group_accumulator = Self::build_accumulator_from_records(&filtered_records);

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
                            // Pass alias context so expressions can reference previously-computed aliases
                            // FR-079 Phase 6: Pass the accumulator for real STDDEV/VARIANCE computation
                            match Self::evaluate_aggregate_expression(
                                expr,
                                &filtered_records,
                                &agg_alias_context,
                                Some(&group_accumulator),
                            ) {
                                Ok(value) => {
                                    debug!(
                                        "AGG: Successfully evaluated field '{}' = {:?}",
                                        field_name, value
                                    );
                                    // Add computed value to alias context for later field references
                                    agg_alias_context.add_alias(field_name.clone(), value.clone());
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
                // Pass alias context so HAVING expressions can reference computed SELECT fields
                let having_result = Self::evaluate_having_expression(
                    having_expr,
                    &filtered_records,
                    &temp_record,
                    &agg_alias_context,
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
            WindowSpec::Rows { .. } => {
                // Phase 8.2: ROWS windows - track last emit time
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
            WindowSpec::Rows { .. } => {
                // Phase 8.2: ROWS windows - track last emit time
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
            WindowSpec::Rows { .. } => {
                // Phase 8.2: ROWS windows - buffer size is managed by window state
                // No special cleanup needed
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
            WindowSpec::Rows { .. } => {
                // Phase 8.2: ROWS windows - buffer size is managed by window state
                // No special cleanup needed
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
        alias_context: &SelectAliasContext,
        context: &ProcessorContext,
    ) -> Result<bool, SqlError> {
        use crate::velostream::sql::ast::{BinaryOperator, Expr};

        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        // Recursively evaluate both sides
                        let left_result = Self::evaluate_having_expression(
                            left,
                            records,
                            temp_record,
                            alias_context,
                            context,
                        )?;
                        let right_result = Self::evaluate_having_expression(
                            right,
                            records,
                            temp_record,
                            alias_context,
                            context,
                        )?;
                        Ok(left_result && right_result)
                    }
                    BinaryOperator::Or => {
                        // Recursively evaluate both sides
                        let left_result = Self::evaluate_having_expression(
                            left,
                            records,
                            temp_record,
                            alias_context,
                            context,
                        )?;
                        let right_result = Self::evaluate_having_expression(
                            right,
                            records,
                            temp_record,
                            alias_context,
                            context,
                        )?;
                        Ok(left_result || right_result)
                    }
                    BinaryOperator::GreaterThanOrEqual
                    | BinaryOperator::GreaterThan
                    | BinaryOperator::LessThanOrEqual
                    | BinaryOperator::LessThan
                    | BinaryOperator::Equal
                    | BinaryOperator::NotEqual => {
                        // Handle comparison operators with aggregate functions
                        // FR-079 Phase 6: Build accumulator for HAVING clause evaluation
                        let having_accumulator = Self::build_accumulator_from_records(records);
                        let left_value = Self::evaluate_aggregate_expression(
                            left,
                            records,
                            alias_context,
                            Some(&having_accumulator),
                        )?;
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
            // Handle Null values in comparisons
            (FieldValue::Null, _, _) | (_, FieldValue::Null, _) => false,
            // For other unhandled type combinations, try a generic field value comparison
            _ => {
                // Default fallback: try generic comparison for same types
                // This handles ScaledInteger and other numeric types
                match (left, right, op) {
                    (
                        FieldValue::ScaledInteger(l, l_scale),
                        FieldValue::ScaledInteger(r, r_scale),
                        op,
                    ) => {
                        // Normalize scales for comparison
                        let (l_val, r_val) = if l_scale == r_scale {
                            (*l, *r)
                        } else if l_scale < r_scale {
                            // Scale left up to match right
                            let scale_diff = (r_scale - l_scale) as u32;
                            (*l * 10_i64.pow(scale_diff), *r)
                        } else {
                            // Scale right up to match left
                            let scale_diff = (l_scale - r_scale) as u32;
                            (*l, *r * 10_i64.pow(scale_diff))
                        };

                        match op {
                            BinaryOperator::GreaterThanOrEqual => l_val >= r_val,
                            BinaryOperator::GreaterThan => l_val > r_val,
                            BinaryOperator::LessThanOrEqual => l_val <= r_val,
                            BinaryOperator::LessThan => l_val < r_val,
                            BinaryOperator::Equal => l_val == r_val,
                            BinaryOperator::NotEqual => l_val != r_val,
                            _ => false,
                        }
                    }
                    // ScaledInteger vs Float
                    (FieldValue::ScaledInteger(l, scale), FieldValue::Float(r), op) => {
                        let l_float = *l as f64 / 10_i64.pow(*scale as u32) as f64;
                        match op {
                            BinaryOperator::GreaterThanOrEqual => l_float >= *r,
                            BinaryOperator::GreaterThan => l_float > *r,
                            BinaryOperator::LessThanOrEqual => l_float <= *r,
                            BinaryOperator::LessThan => l_float < *r,
                            BinaryOperator::Equal => (l_float - r).abs() < f64::EPSILON,
                            BinaryOperator::NotEqual => (l_float - r).abs() >= f64::EPSILON,
                            _ => false,
                        }
                    }
                    // Float vs ScaledInteger
                    (FieldValue::Float(l), FieldValue::ScaledInteger(r, scale), op) => {
                        let r_float = *r as f64 / 10_i64.pow(*scale as u32) as f64;
                        match op {
                            BinaryOperator::GreaterThanOrEqual => *l >= r_float,
                            BinaryOperator::GreaterThan => *l > r_float,
                            BinaryOperator::LessThanOrEqual => *l <= r_float,
                            BinaryOperator::LessThan => *l < r_float,
                            BinaryOperator::Equal => (*l - r_float).abs() < f64::EPSILON,
                            BinaryOperator::NotEqual => (*l - r_float).abs() >= f64::EPSILON,
                            _ => false,
                        }
                    }
                    // ScaledInteger vs Integer
                    (FieldValue::ScaledInteger(l, scale), FieldValue::Integer(r), op) => {
                        let l_float = *l as f64 / 10_i64.pow(*scale as u32) as f64;
                        let r_float = *r as f64;
                        match op {
                            BinaryOperator::GreaterThanOrEqual => l_float >= r_float,
                            BinaryOperator::GreaterThan => l_float > r_float,
                            BinaryOperator::LessThanOrEqual => l_float <= r_float,
                            BinaryOperator::LessThan => l_float < r_float,
                            BinaryOperator::Equal => (l_float - r_float).abs() < f64::EPSILON,
                            BinaryOperator::NotEqual => (l_float - r_float).abs() >= f64::EPSILON,
                            _ => false,
                        }
                    }
                    // Integer vs ScaledInteger
                    (FieldValue::Integer(l), FieldValue::ScaledInteger(r, scale), op) => {
                        let l_float = *l as f64;
                        let r_float = *r as f64 / 10_i64.pow(*scale as u32) as f64;
                        match op {
                            BinaryOperator::GreaterThanOrEqual => l_float >= r_float,
                            BinaryOperator::GreaterThan => l_float > r_float,
                            BinaryOperator::LessThanOrEqual => l_float <= r_float,
                            BinaryOperator::LessThan => l_float < r_float,
                            BinaryOperator::Equal => (l_float - r_float).abs() < f64::EPSILON,
                            BinaryOperator::NotEqual => (l_float - r_float).abs() >= f64::EPSILON,
                            _ => false,
                        }
                    }
                    // For other unhandled combinations, return false
                    _ => false,
                }
            }
        }
    }

    /// Evaluate aggregate expression across multiple records in a window
    fn evaluate_aggregate_expression(
        expr: &crate::velostream::sql::ast::Expr,
        records: &[&StreamRecord],
        alias_context: &SelectAliasContext,
        accumulator: Option<&GroupAccumulator>,
    ) -> Result<FieldValue, SqlError> {
        use crate::velostream::sql::ast::Expr;

        debug!(
            "AGG: evaluate_aggregate_expression called with {} records, expr: {:?}, has_accumulator: {}",
            records.len(),
            expr,
            accumulator.is_some()
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
                    "STDDEV" | "STDDEV_SAMP" | "STDDEV_POP" => {
                        // Standard deviation - requires accumulator data with all numeric values
                        if args.is_empty() {
                            return Err(SqlError::ExecutionError {
                                message: "STDDEV requires exactly one argument".to_string(),
                                query: None,
                            });
                        }

                        // Try to get the field name from the argument
                        let field_name = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => {
                                // If not a simple column, fall back to evaluating without accumulator
                                return ExpressionEvaluator::evaluate_expression_value(
                                    expr,
                                    records.first().unwrap_or(&records[0]),
                                );
                            }
                        };

                        // FR-079 Phase 2: Use accumulator if available
                        if let Some(acc) = accumulator {
                            if let Some(values) = acc.numeric_values.get(&field_name) {
                                if values.len() < 2 {
                                    // Need at least 2 values for sample stddev
                                    return Ok(FieldValue::Null);
                                }

                                let mean = values.iter().sum::<f64>() / values.len() as f64;
                                let variance = if name.to_uppercase() == "STDDEV_POP" {
                                    // Population variance
                                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                        / values.len() as f64
                                } else {
                                    // Sample variance (divid by n-1)
                                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                        / (values.len() - 1) as f64
                                };
                                let stddev = variance.sqrt();
                                debug!(
                                    "AGG: STDDEV computed from accumulator: {} values, mean={}, stddev={}",
                                    values.len(),
                                    mean,
                                    stddev
                                );
                                Ok(FieldValue::Float(stddev))
                            } else {
                                // Field not in accumulator, fall back to single-record evaluation
                                debug!(
                                    "AGG: Field '{}' not found in accumulator, falling back to single-record",
                                    field_name
                                );
                                Ok(FieldValue::Float(0.0))
                            }
                        } else {
                            // No accumulator provided, fall back to regular evaluation (returns 0.0 for single record)
                            ExpressionEvaluator::evaluate_expression_value(
                                expr,
                                records.first().unwrap_or(&records[0]),
                            )
                        }
                    }
                    "VARIANCE" | "VAR_SAMP" | "VAR_POP" => {
                        // Variance - similar to STDDEV but returns variance instead of stddev
                        if args.is_empty() {
                            return Err(SqlError::ExecutionError {
                                message: "VARIANCE requires exactly one argument".to_string(),
                                query: None,
                            });
                        }

                        // Try to get the field name from the argument
                        let field_name = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => {
                                // If not a simple column, fall back to evaluating without accumulator
                                return ExpressionEvaluator::evaluate_expression_value(
                                    expr,
                                    records.first().unwrap_or(&records[0]),
                                );
                            }
                        };

                        // FR-079 Phase 2: Use accumulator if available
                        if let Some(acc) = accumulator {
                            if let Some(values) = acc.numeric_values.get(&field_name) {
                                if values.len() < 2 {
                                    // Need at least 2 values for variance
                                    return Ok(FieldValue::Null);
                                }

                                let mean = values.iter().sum::<f64>() / values.len() as f64;
                                let variance = if name.to_uppercase() == "VAR_POP" {
                                    // Population variance
                                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                        / values.len() as f64
                                } else {
                                    // Sample variance (divide by n-1)
                                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                                        / (values.len() - 1) as f64
                                };
                                debug!(
                                    "AGG: VARIANCE computed from accumulator: {} values, mean={}, variance={}",
                                    values.len(),
                                    mean,
                                    variance
                                );
                                Ok(FieldValue::Float(variance))
                            } else {
                                // Field not in accumulator, fall back to single-record evaluation
                                debug!(
                                    "AGG: Field '{}' not found in accumulator, falling back to single-record",
                                    field_name
                                );
                                Ok(FieldValue::Float(0.0))
                            }
                        } else {
                            // No accumulator provided, fall back to regular evaluation (returns 0.0 for single record)
                            ExpressionEvaluator::evaluate_expression_value(
                                expr,
                                records.first().unwrap_or(&records[0]),
                            )
                        }
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
                // For column references in windowed queries, check alias context first,
                // then fall back to the first record's field value
                debug!(
                    "AGG: Processing column '{}' in aggregate context",
                    column_name
                );

                // Check alias context first (for previously-computed SELECT field aliases)
                if let Some(value) = alias_context.get_alias(column_name) {
                    debug!(
                        "AGG: Found column '{}' in alias context = {:?}",
                        column_name, value
                    );
                    return Ok(value.clone());
                }

                // Fall back to record fields (GROUP BY keys)
                if let Some(first_record) = records.first() {
                    if let Some(value) = first_record.fields.get(column_name) {
                        debug!(
                            "AGG: Found column '{}' in record = {:?}",
                            column_name, value
                        );
                        Ok(value.clone())
                    } else {
                        debug!("AGG: Column '{}' not found, returning Null", column_name);
                        Ok(FieldValue::Null)
                    }
                } else {
                    Ok(FieldValue::Null)
                }
            }
            Expr::BinaryOp { left, right, op } => {
                // FR-079 Phase 3: Handle binary operations with aggregate functions on either side
                debug!(
                    "AGG: Processing binary operation: {:?} {:?} {:?}",
                    left, op, right
                );

                // Recursively evaluate both sides - they may contain aggregates
                let left_value =
                    Self::evaluate_aggregate_expression(left, records, alias_context, accumulator)?;
                let right_value = Self::evaluate_aggregate_expression(
                    right,
                    records,
                    alias_context,
                    accumulator,
                )?;

                // Apply the binary operation
                match op {
                    crate::velostream::sql::ast::BinaryOperator::Add => {
                        // Addition for numeric types
                        match (&left_value, &right_value) {
                            (FieldValue::Integer(l), FieldValue::Integer(r)) => {
                                Ok(FieldValue::Integer(l + r))
                            }
                            (FieldValue::Integer(l), FieldValue::Float(r)) => {
                                Ok(FieldValue::Float(*l as f64 + r))
                            }
                            (FieldValue::Float(l), FieldValue::Integer(r)) => {
                                Ok(FieldValue::Float(l + *r as f64))
                            }
                            (FieldValue::Float(l), FieldValue::Float(r)) => {
                                Ok(FieldValue::Float(l + r))
                            }
                            _ => Ok(FieldValue::Null),
                        }
                    }
                    crate::velostream::sql::ast::BinaryOperator::Subtract => {
                        // Subtraction for numeric types
                        match (&left_value, &right_value) {
                            (FieldValue::Integer(l), FieldValue::Integer(r)) => {
                                Ok(FieldValue::Integer(l - r))
                            }
                            (FieldValue::Integer(l), FieldValue::Float(r)) => {
                                Ok(FieldValue::Float(*l as f64 - r))
                            }
                            (FieldValue::Float(l), FieldValue::Integer(r)) => {
                                Ok(FieldValue::Float(l - *r as f64))
                            }
                            (FieldValue::Float(l), FieldValue::Float(r)) => {
                                Ok(FieldValue::Float(l - r))
                            }
                            _ => Ok(FieldValue::Null),
                        }
                    }
                    crate::velostream::sql::ast::BinaryOperator::Multiply => {
                        // Multiplication for numeric types
                        match (&left_value, &right_value) {
                            (FieldValue::Integer(l), FieldValue::Integer(r)) => {
                                Ok(FieldValue::Integer(l * r))
                            }
                            (FieldValue::Integer(l), FieldValue::Float(r)) => {
                                Ok(FieldValue::Float(*l as f64 * r))
                            }
                            (FieldValue::Float(l), FieldValue::Integer(r)) => {
                                Ok(FieldValue::Float(l * *r as f64))
                            }
                            (FieldValue::Float(l), FieldValue::Float(r)) => {
                                Ok(FieldValue::Float(l * r))
                            }
                            _ => Ok(FieldValue::Null),
                        }
                    }
                    crate::velostream::sql::ast::BinaryOperator::Divide => {
                        // Division for numeric types
                        match (&left_value, &right_value) {
                            (FieldValue::Integer(l), FieldValue::Integer(r)) => {
                                if *r == 0 {
                                    Ok(FieldValue::Null)
                                } else {
                                    Ok(FieldValue::Float(*l as f64 / *r as f64))
                                }
                            }
                            (FieldValue::Integer(l), FieldValue::Float(r)) => {
                                if *r == 0.0 {
                                    Ok(FieldValue::Null)
                                } else {
                                    Ok(FieldValue::Float(*l as f64 / r))
                                }
                            }
                            (FieldValue::Float(l), FieldValue::Integer(r)) => {
                                if *r == 0 {
                                    Ok(FieldValue::Null)
                                } else {
                                    Ok(FieldValue::Float(l / *r as f64))
                                }
                            }
                            (FieldValue::Float(l), FieldValue::Float(r)) => {
                                if *r == 0.0 {
                                    Ok(FieldValue::Null)
                                } else {
                                    Ok(FieldValue::Float(l / r))
                                }
                            }
                            _ => Ok(FieldValue::Null),
                        }
                    }
                    crate::velostream::sql::ast::BinaryOperator::GreaterThanOrEqual
                    | crate::velostream::sql::ast::BinaryOperator::GreaterThan
                    | crate::velostream::sql::ast::BinaryOperator::LessThanOrEqual
                    | crate::velostream::sql::ast::BinaryOperator::LessThan
                    | crate::velostream::sql::ast::BinaryOperator::Equal
                    | crate::velostream::sql::ast::BinaryOperator::NotEqual => {
                        // For comparisons, return a FieldValue::Boolean
                        let comparison_result =
                            Self::compare_field_values(&left_value, &right_value, op);
                        Ok(FieldValue::Boolean(comparison_result))
                    }
                    _ => {
                        // For other operators, try regular evaluation
                        debug!(
                            "AGG: Unsupported binary operator in aggregate expression, falling back to regular evaluation"
                        );
                        if let Some(first_record) = records.first() {
                            ExpressionEvaluator::evaluate_expression_value(expr, first_record)
                        } else {
                            Ok(FieldValue::Null)
                        }
                    }
                }
            }
            _ => {
                // For non-function expressions, try to evaluate with alias context support
                debug!("AGG: Processing non-function expression: {:?}", expr);
                if let Some(first_record) = records.first() {
                    // Use alias-aware evaluator so that CASE WHEN and IN expressions can reference computed aliases
                    ExpressionEvaluator::evaluate_expression_value_with_alias_context(
                        expr,
                        first_record,
                        alias_context,
                    )
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
            WindowSpec::Rows { .. } => false,
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
            WindowSpec::Rows { .. } => false,
            WindowSpec::Session { .. } => {
                // Session windows use different logic
                true
            }
        }
    }

    /// Process a ROWS WINDOW query with bounded buffer and gap detection
    ///
    /// Phase 8.2.2: Handles row-count-based windowing with:
    /// - Fixed-size bounded buffer (VecDeque)
    /// - Optional partition-by key calculation
    /// - Time-gap detection for session boundaries
    /// - Flexible emission strategies (EveryRecord vs BufferFull)
    pub fn process_rows_window(
        query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
        window_spec: &WindowSpec,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Extract ROWS window parameters - match on WindowSpec::Rows to get fields
        match window_spec {
            WindowSpec::Rows {
                buffer_size,
                partition_by: partition_by_opt,
                order_by: order_by_opt,
                window_frame: _,
                emit_mode,
                time_gap,
                expire_after: _,
            } => {
                // Calculate partition key for this record
                // partition_by_opt is &Vec<Expr>, wrap in Some for function signature
                let partition_key =
                    Self::calculate_rows_partition_key(record, Some(partition_by_opt))?;

                // Create state key from query ID and partition
                let state_key = format!("rows_window:{}:{}", query_id, partition_key);

                // Convert Duration to milliseconds for time gap
                let time_gap_ms = time_gap.map(|d| d.as_millis() as i64);

                // Get or create rows window state for this partition
                let rows_state = context.get_or_create_rows_window_state(
                    &state_key,
                    *buffer_size,
                    emit_mode.clone(),
                    time_gap_ms,
                    partition_key.clone(),
                );

                // Extract event time from record (prefer timestamp field or use system time)
                let event_time = Self::extract_event_time_for_rows(record);

                // Add record to buffer and check if it's full
                let buffer_is_full = rows_state.add_record(record.clone(), event_time);

                // Check if we should emit based on emission strategy
                let should_emit = match emit_mode {
                    RowsEmitMode::EveryRecord => true, // Always emit per-record
                    RowsEmitMode::BufferFull => buffer_is_full, // Only when full
                };

                if should_emit {
                    // Update emission tracking
                    rows_state.update_emission(event_time);

                    // Return the current record (window functions will process it with buffer context)
                    Ok(Some(record.clone()))
                } else {
                    // Buffer record but don't emit yet
                    Ok(None)
                }
            }
            _ => {
                return Err(SqlError::ExecutionError {
                    message: "Expected ROWS window specification".to_string(),
                    query: None,
                });
            }
        }
    }

    /// Calculate partition key from PARTITION BY expressions
    fn calculate_rows_partition_key(
        record: &StreamRecord,
        partition_by: Option<&Vec<crate::velostream::sql::ast::Expr>>,
    ) -> Result<String, SqlError> {
        match partition_by {
            Some(exprs) => {
                let keys: Result<Vec<String>, SqlError> = exprs
                    .iter()
                    .map(|expr| {
                        let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
                        Ok(format!("{:?}", value))
                    })
                    .collect();

                Ok(keys?.join("|"))
            }
            None => Ok("__default__".to_string()),
        }
    }

    /// Extract event time from record for rows window processing
    /// Looks for timestamp field, falls back to current system time
    fn extract_event_time_for_rows(record: &StreamRecord) -> i64 {
        // Try to get event_time field first
        if let Some(time_val) = record.get_field("event_time") {
            if let FieldValue::Timestamp(ts) = time_val {
                return ts.timestamp_millis();
            }
        }

        // Try timestamp field
        if let Some(time_val) = record.get_field("timestamp") {
            if let FieldValue::Timestamp(ts) = time_val {
                return ts.timestamp_millis();
            }
        }

        // Fall back to current system time
        chrono::Utc::now().timestamp_millis()
    }

    /// Compute RANK for current row within the window
    /// RANK: 1, 2, 2, 4, 5 (with gaps)
    fn compute_rank(
        rows_state: &RowsWindowState,
        current_row_index: usize,
    ) -> Result<i64, SqlError> {
        // Find the rank based on the ranking index
        // Iterate through ranking_index to find which rank position this row is at
        let mut rank = 1i64;
        let mut row_count = 0;

        for (_, indices) in &rows_state.ranking_index {
            for idx in indices {
                row_count += 1;
                if *idx == current_row_index {
                    return Ok(rank);
                }
                // Check if we've processed all rows in this rank level
            }
            rank += indices.len() as i64;
        }

        Ok(rank)
    }

    /// Compute DENSE_RANK for current row within the window
    /// DENSE_RANK: 1, 2, 2, 3, 4 (no gaps)
    fn compute_dense_rank(
        rows_state: &RowsWindowState,
        current_row_index: usize,
    ) -> Result<i64, SqlError> {
        // Find the dense rank based on the ranking index
        let mut dense_rank = 1i64;

        for (_, indices) in &rows_state.ranking_index {
            for idx in indices {
                if *idx == current_row_index {
                    return Ok(dense_rank);
                }
            }
            dense_rank += 1;
        }

        Ok(dense_rank)
    }

    /// Compute PERCENT_RANK for current row within the window
    /// PERCENT_RANK: (rank - 1) / (total_rows - 1), returns 0.0 to 1.0
    fn compute_percent_rank(
        rows_state: &RowsWindowState,
        current_row_index: usize,
    ) -> Result<f64, SqlError> {
        let rank = Self::compute_rank(rows_state, current_row_index)?;
        let total_rows = rows_state.row_buffer.len() as i64;

        if total_rows <= 1 {
            Ok(0.0)
        } else {
            Ok((rank as f64 - 1.0) / (total_rows as f64 - 1.0))
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
            WindowSpec::Rows { .. } => None, // Rows windows don't have explicit time columns
            WindowSpec::Session { .. } => None, // Session windows don't have explicit time columns
        }
    }
}
