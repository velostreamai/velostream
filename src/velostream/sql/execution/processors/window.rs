//! Window Query Processor
//!
//! Handles windowed query processing through the window_v2 architecture.
//! FR-081 Phase 2E: Legacy implementation removed - all queries route to window_v2.

use super::{ProcessorContext, WindowContext};
use crate::velostream::sql::ast::WindowSpec;
use crate::velostream::sql::execution::expression::is_aggregate_function;
use crate::velostream::sql::execution::types::StreamRecord;
use crate::velostream::sql::{SqlError, StreamingQuery};
use log::{error, trace};

/// Window processing router - delegates all work to window_v2
pub struct WindowProcessor;

impl WindowProcessor {
    /// Process a windowed query using window_v2 architecture (FR-081 Phase 2A+)
    ///
    /// This is the entry point for all window processing. It routes queries to the
    /// window_v2 trait-based architecture which supports:
    /// - Tumbling, Sliding, Session, and Rows windows
    /// - GROUP BY aggregations (AVG, MIN, MAX, COUNT, SUM, etc.)
    /// - EMIT CHANGES and EMIT FINAL modes
    /// - Watermark-based late data handling
    ///
    /// # Arguments
    ///
    /// * `query_id` - Unique identifier for this query
    /// * `query` - The streaming query to execute
    /// * `record` - The incoming stream record
    /// * `context` - Execution context with state management
    /// * `source_id` - Optional source identifier (unused, kept for API compat)
    ///
    /// # Returns
    ///
    /// - `Ok(Some(StreamRecord))` - Window emitted a result record
    /// - `Ok(None)` - Window buffered the record, no emission yet
    /// - `Err(SqlError)` - Processing error occurred
    ///
    /// # FR-081 Phase 2E
    ///
    /// Legacy window processing path completely removed. All queries must use
    /// window_v2 architecture (enabled by default in all configurations).
    pub fn process_windowed_query_enhanced(
        query_id: &str,
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
        _source_id: Option<&str>, // Deprecated parameter, kept for API compatibility
    ) -> Result<Option<StreamRecord>, SqlError> {
        // FR-081 Phase 2E+: window_v2 is the only window architecture available
        // The legacy window implementation was removed in Phase 2E
        trace!("Processing query {} with window_v2 architecture", query_id);

        crate::velostream::sql::execution::window_v2::adapter::WindowAdapter::process_with_v2(
            query_id, query, record, context,
        )
    }

    /// Check if query has aggregation functions (AVG, MIN, MAX, COUNT, SUM, etc.)
    ///
    /// Used by the router to provide informative error messages.
    fn query_has_aggregations(query: &StreamingQuery) -> bool {
        use crate::velostream::sql::ast::SelectField;

        if let StreamingQuery::Select { fields, .. } = query {
            for field in fields {
                match field {
                    SelectField::Column(_) => continue,
                    SelectField::AliasedColumn { .. } => continue,
                    SelectField::Wildcard => continue,
                    SelectField::Expression { expr, .. } => {
                        if Self::expr_is_aggregation(expr) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /// Recursively check if an expression contains aggregation functions
    fn expr_is_aggregation(expr: &crate::velostream::sql::ast::Expr) -> bool {
        use crate::velostream::sql::ast::Expr;

        match expr {
            Expr::Function { name, .. } => is_aggregate_function(name),
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_is_aggregation(left) || Self::expr_is_aggregation(right)
            }
            _ => false,
        }
    }

    /// Create a window context (utility function)
    pub fn create_window_context() -> WindowContext {
        WindowContext {
            buffer: Vec::new(),
            last_emit: 0,
            should_emit: false,
        }
    }

    /// Extract time column from window spec (utility function)
    pub fn get_time_column(window_spec: &WindowSpec) -> Option<&str> {
        window_spec.time_column()
    }

    /// Extract GROUP BY columns from query (utility function)
    ///
    /// Used by tests and window_v2 adapter for query analysis.
    pub fn get_group_by_columns(query: &StreamingQuery) -> Option<Vec<String>> {
        use crate::velostream::sql::ast::Expr;

        if let StreamingQuery::Select { group_by, .. } = query {
            if let Some(group_exprs) = group_by {
                // Extract column names from GROUP BY expressions
                let columns: Vec<String> = group_exprs
                    .iter()
                    .filter_map(|expr| {
                        if let Expr::Column(col) = expr {
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

    /// Check if query has EMIT CHANGES clause (utility function)
    ///
    /// Used by tests and window_v2 adapter for query analysis.
    pub fn is_emit_changes(query: &StreamingQuery) -> bool {
        use crate::velostream::sql::ast::{EmitMode, RowsEmitMode};

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

    /// Check if window is ready for emission based on watermarks (utility function)
    ///
    /// Used by watermark tests to verify emission timing.
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
}
