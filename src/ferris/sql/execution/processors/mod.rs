//! Query Processing Modules
//!
//! This module contains specialized processors for different types of SQL operations:
//! - SELECT processing
//! - Window processing
//! - JOIN processing
//! - LIMIT processing

use crate::ferris::sql::execution::{FieldValue, StreamRecord};
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
pub struct ProcessorContext {
    /// Current record count for limit checking
    pub record_count: u64,
    /// Maximum record count (for LIMIT)
    pub max_records: Option<u64>,
    /// Window processing state
    pub window_context: Option<WindowContext>,
    /// JOIN processing utilities
    pub join_context: JoinContext,
    /// GROUP BY processing state
    pub group_by_states: HashMap<String, crate::ferris::sql::execution::internal::GroupByState>,
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

/// JOIN processing context and utilities
pub struct JoinContext;

impl JoinContext {
    pub fn get_right_record(
        &self,
        source: &crate::ferris::sql::ast::StreamSource,
        _window: &Option<crate::ferris::sql::ast::JoinWindow>,
    ) -> Result<Option<StreamRecord>, SqlError> {
        // Implementation moved from engine.rs - delegate to JoinProcessor
        match source {
            crate::ferris::sql::ast::StreamSource::Stream(name)
            | crate::ferris::sql::ast::StreamSource::Table(name) => {
                if name == "empty_stream" {
                    // Simulate no matching record
                    return Ok(None);
                }

                // Create mock record
                Ok(Some(JoinProcessor::create_mock_right_record(source)?))
            }
            crate::ferris::sql::ast::StreamSource::Subquery(_) => {
                // Subqueries not yet supported for JOIN operations
                Err(SqlError::ExecutionError {
                    message: "Subqueries in JOIN operations not yet supported".to_string(),
                    query: None,
                })
            }
        }
    }
}

// Re-export processor modules
pub use self::join::JoinProcessor;
pub use self::limit::LimitProcessor;
pub use self::select::SelectProcessor;
pub use self::window::WindowProcessor;

// Internal processor modules
mod join;
mod limit;
mod select;
mod window;

// SHOW processor (simple inline implementation)
struct ShowProcessor;

impl ShowProcessor {
    fn process(
        query: &StreamingQuery,
        record: &StreamRecord,
        _context: &mut ProcessorContext,
    ) -> Result<ProcessorResult, SqlError> {
        if let StreamingQuery::Show {
            resource_type,
            pattern,
        } = query
        {
            let mut fields = HashMap::new();
            fields.insert(
                "show_type".to_string(),
                FieldValue::String(format!("{:?}", resource_type)),
            );
            if let Some(p) = pattern {
                fields.insert("pattern".to_string(), FieldValue::String(p.clone()));
            }

            let result_record = StreamRecord {
                fields,
                timestamp: record.timestamp,
                offset: record.offset,
                partition: record.partition,
                headers: record.headers.clone(),
            };

            Ok(ProcessorResult {
                record: Some(result_record),
                header_mutations: Vec::new(),
                should_count: true,
            })
        } else {
            Err(SqlError::ExecutionError {
                message: "Invalid query type for ShowProcessor".to_string(),
                query: None,
            })
        }
    }
}
