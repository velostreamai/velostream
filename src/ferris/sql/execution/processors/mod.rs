//! Query Processing Modules
//!
//! This module contains specialized processors for different types of SQL operations:
//! - SELECT processing
//! - Window processing
//! - JOIN processing
//! - LIMIT processing
//! - SHOW/DESCRIBE processing

use crate::ferris::sql::execution::performance::PerformanceMonitor;
use crate::ferris::sql::execution::StreamRecord;
use crate::ferris::sql::{SqlError, StreamingQuery};

pub mod context;
pub mod processor_types;

pub use context::{ProcessorContext, WindowContext};
pub use processor_types::{HeaderMutation, HeaderOperation, ProcessorResult};

/// Main processor coordination interface
pub struct QueryProcessor;

impl QueryProcessor {
    /// Process a query against a record using the appropriate processor
    pub fn process_query(
        _query: &StreamingQuery,
        _record: &StreamRecord,
        _context: &mut ProcessorContext,
    ) -> Result<ProcessorResult, SqlError> {
        // Simplified implementation - actual query processing logic is in specialized processors
        Ok(ProcessorResult {
            record: None,
            header_mutations: Vec::new(),
            should_count: true,
        })
    }

    /// Process a query with optional performance monitoring
    pub fn process_query_with_monitoring(
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
        _performance_monitor: Option<&PerformanceMonitor>,
    ) -> Result<ProcessorResult, SqlError> {
        // Use simplified processing for now
        Self::process_query(query, record, context)
    }

    /// Get processor metrics for monitoring
    pub fn get_metrics(context: &ProcessorContext) -> std::collections::HashMap<String, u64> {
        let mut metrics = std::collections::HashMap::new();
        metrics.insert("record_count".to_string(), context.record_count);

        if let Some(max) = context.max_records {
            metrics.insert("max_records".to_string(), max);
        }

        metrics.insert(
            "data_sources".to_string(),
            context.data_sources.len() as u64,
        );
        metrics.insert("schemas".to_string(), context.schemas.len() as u64);
        metrics.insert(
            "stream_handles".to_string(),
            context.stream_handles.len() as u64,
        );

        // Add pluggable data source metrics
        metrics.insert(
            "data_readers".to_string(),
            context.data_readers.len() as u64,
        );
        metrics.insert(
            "data_writers".to_string(),
            context.data_writers.len() as u64,
        );
        metrics.insert(
            "persistent_window_states".to_string(),
            context.persistent_window_states.len() as u64,
        );
        metrics.insert(
            "dirty_window_states_count".to_string(),
            context.dirty_window_states.count_ones() as u64,
        );

        metrics
    }

    /// Clear context state for fresh processing
    pub fn reset_context(context: &mut ProcessorContext) {
        context.record_count = 0;
        context.window_context = None;
        context.data_sources.clear();
    }

    /// Validate context readiness for processing
    pub fn validate_context(context: &ProcessorContext) -> Result<(), SqlError> {
        // Validate that we have active data sources if needed
        if context.data_readers.is_empty() && context.data_sources.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "No data sources available in context".to_string(),
                query: None,
            });
        }

        // Validate window states are not corrupted
        if context.persistent_window_states.len() > 32 {
            return Err(SqlError::ExecutionError {
                message: "Too many persistent window states (max 32 supported)".to_string(),
                query: None,
            });
        }

        Ok(())
    }
}

// Re-export join context
pub use self::join_context::JoinContext;

// Re-export processor modules
pub use self::delete::DeleteProcessor;
pub use self::insert::InsertProcessor;
pub use self::join::JoinProcessor;
pub use self::limit::LimitProcessor;
pub use self::select::SelectProcessor;
pub use self::show::ShowProcessor;
pub use self::update::UpdateProcessor;
pub use self::window::WindowProcessor;

// Re-export sub-modules for direct access
pub mod delete;
pub mod insert;
pub mod job;
pub mod join;
pub mod join_context;
pub mod limit;
pub mod select;
pub mod show;
pub mod update;
pub mod window;
