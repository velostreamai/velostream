//! Streaming SQL Execution Engine
//!
//! This module implements the execution engine for streaming SQL queries. It processes
//! SQL AST nodes and executes them against streaming data records, supporting real-time
//! query evaluation with expression processing, filtering, and basic aggregations.
//!
//! ## Public API
//!
//! The primary interface for executing SQL queries against streaming data:
//!
//! - [`StreamExecutionEngine`] - Main execution engine
//! - [`StreamRecord`] - Input record format
//! - [`FieldValue`] - Value type system
//!
//! ## Usage
//!
//! ```rust
//! use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
//!
//! let engine = StreamExecutionEngine::new(output_sender, serialization_format);
//! engine.execute(&query, record).await?;
//! ```
//!
//! All other types and methods are internal implementation details.

pub mod engine;
pub mod internal;
pub mod types;

// Re-export public API only
pub use engine::StreamExecutionEngine;
pub use types::{FieldValue, StreamRecord};

// Internal types are available within this module but not re-exported publicly
pub(crate) use internal::{
    ExecutionMessage, ExecutionState, GroupAccumulator, GroupByState, HeaderMutation,
    HeaderOperation, QueryExecution, WindowState,
};
