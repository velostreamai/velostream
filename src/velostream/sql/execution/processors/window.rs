//! Window Query Processor
//!
//! Handles windowed query processing through the window_v2 architecture.
//! FR-081 Phase 2E: Legacy implementation removed - all queries route to window_v2.

use super::{ProcessorContext, WindowContext};
use crate::velostream::sql::ast::WindowSpec;
use crate::velostream::sql::execution::types::StreamRecord;
use crate::velostream::sql::{SqlError, StreamingQuery};
use log::{debug, error};

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
        // FR-081 Phase 2A+: Route to window_v2 architecture (supports all aggregations)
        if context.is_window_v2_enabled() {
            debug!(
                "FR-081 Phase 2E: Processing query {} with window_v2 architecture",
                query_id
            );
            return crate::velostream::sql::execution::window_v2::adapter::WindowAdapter::process_with_v2(
                query_id, query, record, context,
            );
        }

        // FR-081 Phase 2E: window_v2 is always enabled, this path should never be reached
        error!(
            "FR-081 Phase 2E ERROR: window_v2 disabled for query: {}. \
             Legacy window processing has been removed. Enable window_v2 in ExecutionConfig.",
            query_id
        );

        Err(SqlError::ExecutionError {
            message: format!(
                "Window processing requires window_v2 architecture. \
                 Legacy implementation removed in Phase 2E. Query: {}",
                query_id
            ),
            query: Some(format!("{:?}", query)),
        })
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
            Expr::Function { name, .. } => {
                let name_upper = name.to_uppercase();
                matches!(
                    name_upper.as_str(),
                    "AVG" | "MIN" | "MAX" | "COUNT" | "SUM" | "STDDEV" | "VARIANCE"
                )
            }
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
}
