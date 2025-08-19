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
//! ```rust,no_run
//! # use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
//! # use ferrisstreams::ferris::serialization::JsonFormat;
//! # use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
//! # use std::sync::Arc;
//! # use tokio::sync::mpsc;
//! # use std::collections::HashMap;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let (output_sender, _receiver) = mpsc::unbounded_channel();
//! let serialization_format = Arc::new(JsonFormat);
//! let mut engine = StreamExecutionEngine::new(output_sender, serialization_format);
//!
//! // Parse a simple query and execute with a record
//! let parser = StreamingSqlParser::new();
//! let query = parser.parse("SELECT * FROM stream")?;
//! let record = HashMap::new(); // Empty record for example
//! engine.execute(&query, record).await?;
//! # Ok(())
//! # }
//! ```
//!
//! All other types and methods are internal implementation details.

pub mod aggregation;
pub mod engine;
pub mod expression;
pub mod internal;
pub mod types;

// Re-export public API only
pub use engine::StreamExecutionEngine;
pub use types::{FieldValue, StreamRecord};

// Internal types are available within this module but not re-exported publicly
