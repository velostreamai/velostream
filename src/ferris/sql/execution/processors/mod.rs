//! Query Processing Modules
//!
//! This module contains specialized processors for different types of SQL operations:
//! - SELECT processing
//! - Window processing
//! - JOIN processing
//! - LIMIT processing
//! - SHOW/DESCRIBE processing

use crate::ferris::sql::execution::StreamRecord;
use crate::ferris::sql::execution::internal::WindowState;
use crate::ferris::sql::schema::{Schema, StreamHandle};
use crate::ferris::sql::{SqlError, StreamingQuery};
use std::collections::HashMap;

/// Main processor coordination interface
pub struct QueryProcessor;

/// Result of query processing
#[derive(Debug, Clone)]
pub struct ProcessorResult {
    /// The processed record, if any
    pub record: Option<StreamRecord>,
    /// Any header mutations to apply
    pub header_mutations: Vec<HeaderMutation>,
    /// Whether the record count should be incremented
    pub should_count: bool,
}

/// Header mutation operation
#[derive(Debug, Clone)]
pub struct HeaderMutation {
    pub key: String,
    pub operation: HeaderOperation,
    pub value: Option<String>,
}

/// Types of header operations
#[derive(Debug, Clone)]
pub enum HeaderOperation {
    Set,
    Remove,
}

impl QueryProcessor {
    /// Process a query against a record using the appropriate processor
    pub fn process_query(
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<ProcessorResult, SqlError> {
        match query {
            StreamingQuery::Select { .. } => SelectProcessor::process(query, record, context),
            StreamingQuery::CreateStream { as_select, .. } => {
                // Process the underlying SELECT query
                SelectProcessor::process(as_select, record, context)
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                // Process the underlying SELECT query for materialized table
                SelectProcessor::process(as_select, record, context)
            }
            StreamingQuery::Show { .. } => {
                // SHOW commands return metadata
                ShowProcessor::process(query, record, context)
            }
            StreamingQuery::InsertInto {
                table_name,
                columns,
                source,
            } => {
                // Process INSERT INTO statement
                match InsertProcessor::process_insert(table_name, columns, source, record) {
                    Ok(insert_records) => {
                        // Return the first insert record (or None if empty)
                        // TODO: Handle multiple insert records properly
                        let result_record = insert_records.into_iter().next();
                        Ok(ProcessorResult {
                            record: result_record,
                            header_mutations: Vec::new(),
                            should_count: true,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            StreamingQuery::Update {
                table_name,
                assignments,
                where_clause,
            } => {
                // Process UPDATE statement
                match UpdateProcessor::process_update(table_name, assignments, where_clause, record)
                {
                    Ok(updated_record) => {
                        let should_count = updated_record.is_some();
                        Ok(ProcessorResult {
                            record: updated_record,
                            header_mutations: Vec::new(),
                            should_count,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            StreamingQuery::Delete {
                table_name,
                where_clause,
            } => {
                // Process DELETE statement
                match DeleteProcessor::process_delete(table_name, where_clause, record) {
                    Ok(tombstone_record) => {
                        let should_count = tombstone_record.is_some();
                        Ok(ProcessorResult {
                            record: tombstone_record,
                            header_mutations: Vec::new(),
                            should_count,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            _ => {
                // Handle other query types with placeholder
                Err(SqlError::ExecutionError {
                    message: "Query type not yet supported by QueryProcessor".to_string(),
                    query: None,
                })
            }
        }
    }
}

/// Context passed to processors containing shared state and utilities
/// Optimized for high-performance multi-threading scenarios
pub struct ProcessorContext {
    /// Current record count for limit checking
    pub record_count: u64,
    /// Maximum record count (for LIMIT)
    pub max_records: Option<u64>,
    /// Window processing state (legacy - kept for compatibility)
    pub window_context: Option<WindowContext>,
    /// JOIN processing utilities
    pub join_context: JoinContext,
    /// GROUP BY processing state
    pub group_by_states: HashMap<String, crate::ferris::sql::execution::internal::GroupByState>,
    /// Schema registry for introspection (SHOW/DESCRIBE operations)
    pub schemas: HashMap<String, Schema>,
    /// Stream handles registry
    pub stream_handles: HashMap<String, StreamHandle>,
    /// Data sources for subquery execution
    /// Maps table/stream name to available records for querying
    pub data_sources: HashMap<String, Vec<StreamRecord>>,

    // === HIGH-PERFORMANCE WINDOW STATE MANAGEMENT ===
    /// Persistent window states for queries processed in this context
    /// Using Vec for cache efficiency - most contexts handle 1-2 queries
    pub persistent_window_states: Vec<(String, WindowState)>,
    /// Track which states were modified for efficient persistence (bit mask)
    pub dirty_window_states: u32,
}

impl ProcessorContext {
    /// Set data sources for subquery execution
    /// This allows external systems to populate the context with available data
    pub fn set_data_sources(&mut self, data_sources: HashMap<String, Vec<StreamRecord>>) {
        self.data_sources = data_sources;
    }

    /// Add a single data source for subquery execution
    pub fn add_data_source(&mut self, source_name: String, records: Vec<StreamRecord>) {
        self.data_sources.insert(source_name, records);
    }

    /// Check if a data source exists
    pub fn has_data_source(&self, source_name: &str) -> bool {
        self.data_sources.contains_key(source_name)
    }

    // === HIGH-PERFORMANCE WINDOW STATE METHODS ===

    /// Get or create a window state for a query (O(1) for small contexts, optimized for threading)
    pub fn get_or_create_window_state(
        &mut self,
        query_id: &str,
        window_spec: &crate::ferris::sql::ast::WindowSpec,
    ) -> &mut WindowState {
        // Check if window state already exists
        for (idx, (stored_query_id, _)) in self.persistent_window_states.iter().enumerate() {
            if stored_query_id == query_id {
                // Mark as dirty for persistence
                if idx < 32 {
                    // Protect against bit mask overflow
                    self.dirty_window_states |= 1 << (idx as u32);
                }
                // Return mutable reference (separate borrow)
                return &mut self.persistent_window_states[idx].1;
            }
        }

        // Create new state if not found (happens rarely)
        let new_state = WindowState::new(window_spec.clone());
        let new_idx = self.persistent_window_states.len();
        self.persistent_window_states
            .push((query_id.to_string(), new_state));

        // Mark new state as dirty
        if new_idx < 32 {
            // Protect against bit mask overflow
            self.dirty_window_states |= 1 << (new_idx as u32);
        }

        &mut self.persistent_window_states[new_idx].1
    }

    /// Get window state if it exists (read-only, no dirty marking)
    pub fn get_window_state(&self, query_id: &str) -> Option<&WindowState> {
        self.persistent_window_states
            .iter()
            .find(|(stored_query_id, _)| stored_query_id == query_id)
            .map(|(_, window_state)| window_state)
    }

    /// Load window states from engine (called during context creation)
    pub fn load_window_states(&mut self, states: Vec<(String, WindowState)>) {
        self.persistent_window_states = states;
        self.dirty_window_states = 0; // Start clean
    }

    /// Get modified window states for persistence (returns only changed states)
    pub fn get_dirty_window_states(&self) -> Vec<(String, WindowState)> {
        let mut dirty_states = Vec::new();

        for (idx, (query_id, window_state)) in self.persistent_window_states.iter().enumerate() {
            if idx < 32 && (self.dirty_window_states & (1 << idx)) != 0 {
                dirty_states.push((query_id.clone(), window_state.clone()));
            }
        }

        dirty_states
    }

    /// Clear dirty flags (called after persistence)
    pub fn clear_dirty_flags(&mut self) {
        self.dirty_window_states = 0;
    }
}

/// Window processing context
pub struct WindowContext {
    /// Buffered records for windowing
    pub buffer: Vec<StreamRecord>,
    /// Last emission time
    pub last_emit: i64,
    /// Should emit in this processing cycle
    pub should_emit: bool,
}

// Import join context
pub use self::join_context::JoinContext;

// Re-export processor modules
pub use self::delete::DeleteProcessor;
pub use self::insert::InsertProcessor;
pub use self::join::JoinProcessor;
pub use self::limit::LimitProcessor;
pub use self::select::SelectProcessor;
pub use self::update::UpdateProcessor;
pub use self::window::WindowProcessor;

// Internal processor modules
mod delete;
mod insert;
mod join;
mod join_context;
mod limit;
mod select;
mod show;
mod update;
mod window;

// Import processors
use self::show::ShowProcessor;
