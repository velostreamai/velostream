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

use super::strategies::{
    RowsWindowStrategy, SessionWindowStrategy, SlidingWindowStrategy, TumblingWindowStrategy,
};
use super::emission::{EmitChangesStrategy, EmitFinalStrategy};
use super::traits::{EmissionStrategy, WindowStrategy};
use super::types::SharedRecord;
use crate::velostream::sql::ast::{EmitMode, RowsEmitMode, WindowSpec, StreamingQuery, SelectField};
use crate::velostream::sql::execution::{StreamRecord, FieldValue};
use crate::velostream::sql::execution::processors::ProcessorContext;
use crate::velostream::sql::SqlError;
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
        if let StreamingQuery::Select { window, .. } = query {
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
        context.metadata.contains_key(state_key)
    }

    /// Initialize window_v2 state for a new query
    fn initialize_v2_state(
        context: &mut ProcessorContext,
        state_key: &str,
        window_spec: &WindowSpec,
        query: &StreamingQuery,
    ) -> Result<(), SqlError> {
        // Mark that we have v2 state for this query
        context.metadata.insert(state_key.to_string(), "initialized".to_string());

        // We'll store actual state in a separate HashMap structure
        // For Phase 2A.3, we'll add a new field to ProcessorContext
        Ok(())
    }

    /// Process a record through the window strategy
    fn process_record_with_strategy(
        context: &mut ProcessorContext,
        state_key: &str,
        record: SharedRecord,
        query: &StreamingQuery,
        window_spec: &WindowSpec,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // This is where we'll integrate with window_v2 strategies
        // For now, return None to indicate no emission
        // Full implementation will follow in subsequent steps
        Ok(None)
    }

    /// Create a window strategy based on the window specification
    pub fn create_strategy(window_spec: &WindowSpec) -> Result<Box<dyn WindowStrategy>, SqlError> {
        match window_spec {
            WindowSpec::Tumbling { size, time_column } => {
                let window_size_ms = size.as_millis() as i64;
                Ok(Box::new(TumblingWindowStrategy::new(
                    window_size_ms,
                    time_column.clone().unwrap_or_else(|| "event_time".to_string()),
                )))
            }
            WindowSpec::Sliding { size, advance, time_column } => {
                let window_size_ms = size.as_millis() as i64;
                let advance_ms = advance.as_millis() as i64;
                Ok(Box::new(SlidingWindowStrategy::new(
                    window_size_ms,
                    advance_ms,
                    time_column.clone().unwrap_or_else(|| "event_time".to_string()),
                )))
            }
            WindowSpec::Session { gap, time_column, .. } => {
                let gap_ms = gap.as_millis() as i64;
                Ok(Box::new(SessionWindowStrategy::new(
                    gap_ms,
                    time_column.clone().unwrap_or_else(|| "event_time".to_string()),
                )))
            }
            WindowSpec::Rows { buffer_size, emit_mode, .. } => {
                let emit_per_record = matches!(emit_mode, RowsEmitMode::EveryRecord);
                Ok(Box::new(RowsWindowStrategy::new(*buffer_size as usize, emit_per_record)))
            }
        }
    }

    /// Create an emission strategy based on the query
    pub fn create_emission_strategy(query: &StreamingQuery) -> Result<Box<dyn EmissionStrategy>, SqlError> {
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
        if let StreamingQuery::Select { emit_mode, window, .. } = query {
            // Check emit_mode field on the SELECT query
            if let Some(mode) = emit_mode {
                matches!(mode, EmitMode::Changes)
            } else {
                // For ROWS windows, check the emit_mode in the window spec
                if let Some(WindowSpec::Rows { emit_mode: rows_emit, .. }) = window {
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
