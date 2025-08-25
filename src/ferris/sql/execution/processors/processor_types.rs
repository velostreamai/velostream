//! Basic processor types and result structures

use crate::ferris::sql::execution::StreamRecord;

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